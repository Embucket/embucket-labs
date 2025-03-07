// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::models::ColumnInfo;
use arrow::array::{
    Array, Decimal128Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use sqlparser::ast::{Ident, ObjectName};
use std::collections::HashMap;
use std::sync::Arc;
use strum::{Display, EnumString};

// This isn't the best way to do this, but it'll do for now
// TODO: Revisit
pub struct Config {
    pub dbt_serialization_format: DataSerializationFormat,
}

impl Config {
    pub fn new(data_format: &str) -> Result<Self, strum::ParseError> {
        Ok(Self {
            dbt_serialization_format: DataSerializationFormat::try_from(data_format)?,
        })
    }
}
#[derive(Copy, Clone, PartialEq, Eq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum DataSerializationFormat {
    Arrow,
    Json,
}

/*#[async_trait::async_trait]
pub trait S3ClientValidation: Send + Sync {
    async fn get_aws_bucket_acl(
        &self,
        request: GetBucketAclRequest,
    ) -> ControlPlaneResult<GetBucketAclOutput>;
}

#[async_trait::async_trait]
impl S3ClientValidation for S3Client {
    async fn get_aws_bucket_acl(
        &self,
        request: GetBucketAclRequest,
    ) -> ControlPlaneResult<GetBucketAclOutput> {
        self.client
            .get_bucket_acl(request)
            .await
            .map_err(ControlPlaneError::from)
    }
}

pub struct S3Client {
    client: ExternalS3Client,
}

impl S3Client {
    pub fn new(profile: &StorageProfile) -> ControlPlaneResult<Self> {
        if let Some(credentials) = profile.credentials.clone() {
            match credentials {
                Credentials::AccessKey(creds) => {
                    let profile_region = profile.region.clone().unwrap_or_default();
                    let credentials = StaticProvider::new_minimal(
                        creds.aws_access_key_id.clone(),
                        creds.aws_secret_access_key,
                    );
                    let region = Region::Custom {
                        name: profile_region.clone(),
                        endpoint: profile.endpoint.clone().unwrap_or_else(|| {
                            format!("https://s3.{profile_region}.amazonaws.com")
                        }),
                    };

                    let dispatcher =
                        HttpClient::new().context(crate::error::InvalidTLSConfigurationSnafu)?;
                    Ok(Self {
                        client: ExternalS3Client::new_with(dispatcher, credentials, region),
                    })
                }
                Credentials::Role(_) => Err(ControlPlaneError::UnsupportedAuthenticationMethod {
                    method: credentials.to_string(),
                }),
            }
        } else {
            Err(ControlPlaneError::InvalidCredentials)
        }
    }
}*/

#[must_use]
pub fn first_non_empty_type(union_array: &UnionArray) -> Option<(DataType, ArrayRef)> {
    for i in 0..union_array.type_ids().len() {
        let type_id = union_array.type_id(i);
        let child = union_array.child(type_id);
        if child.len() > 0 {
            return Some((child.data_type().clone(), Arc::clone(child)));
        }
    }
    None
}

pub fn convert_record_batches(
    records: Vec<RecordBatch>,
    data_format: DataSerializationFormat,
) -> DataFusionResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
    let mut converted_batches = Vec::new();
    let column_infos = ColumnInfo::from_batch(&records);

    for batch in records {
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        for (i, column) in batch.columns().iter().enumerate() {
            let metadata = column_infos[i].to_metadata();
            let field = batch.schema().field(i).clone();
            let converted_column = match field.data_type() {
                DataType::Union(..) => {
                    if let Some(union_array) = column.as_any().downcast_ref::<UnionArray>() {
                        if let Some((data_type, array)) = first_non_empty_type(union_array) {
                            fields.push(
                                Field::new(field.name(), data_type, field.is_nullable())
                                    .with_metadata(metadata),
                            );
                            array
                        } else {
                            fields.push(field.with_metadata(metadata));
                            Arc::clone(column)
                        }
                    } else {
                        fields.push(field.with_metadata(metadata));
                        Arc::clone(column)
                    }
                }
                DataType::Timestamp(unit, _) => {
                    let converted_column = convert_timestamp_to_struct(column, *unit, data_format);
                    fields.push(
                        Field::new(
                            field.name(),
                            converted_column.data_type().clone(),
                            field.is_nullable(),
                        )
                        .with_metadata(metadata),
                    );
                    Arc::clone(&converted_column)
                }
                DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                    let column_info = &column_infos[i];
                    convert_uint_to_int_datatypes(
                        &mut fields,
                        &field,
                        column,
                        metadata,
                        data_format,
                        (
                            column_info.precision.unwrap_or(38),
                            column_info.scale.unwrap_or(0),
                        ),
                    )
                }
                _ => {
                    fields.push(field.clone().with_metadata(metadata));
                    Arc::clone(column)
                }
            };
            columns.push(converted_column);
        }
        let new_schema = Arc::new(Schema::new(fields));
        let converted_batch = RecordBatch::try_new(new_schema, columns)?;
        converted_batches.push(converted_batch);
    }

    Ok((converted_batches.clone(), column_infos))
}

macro_rules! downcast_and_iter {
    ($column:expr, $array_type:ty) => {
        $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap()
            .into_iter()
    };
}

#[allow(
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation
)]
fn convert_timestamp_to_struct(
    column: &ArrayRef,
    unit: TimeUnit,
    data_format: DataSerializationFormat,
) -> ArrayRef {
    match data_format {
        DataSerializationFormat::Arrow => {
            let timestamps: Vec<_> = match unit {
                TimeUnit::Second => downcast_and_iter!(column, TimestampSecondArray).collect(),
                TimeUnit::Millisecond => {
                    downcast_and_iter!(column, TimestampMillisecondArray).collect()
                }
                TimeUnit::Microsecond => {
                    downcast_and_iter!(column, TimestampMicrosecondArray).collect()
                }
                TimeUnit::Nanosecond => {
                    downcast_and_iter!(column, TimestampNanosecondArray).collect()
                }
            };
            Arc::new(Int64Array::from(timestamps)) as ArrayRef
        }
        DataSerializationFormat::Json => {
            let timestamps: Vec<_> = match unit {
                TimeUnit::Second => downcast_and_iter!(column, TimestampSecondArray)
                    .map(|x| {
                        x.map(|ts| {
                            let ts = DateTime::from_timestamp(ts, 0).unwrap();
                            format!("{}", ts.timestamp())
                        })
                    })
                    .collect(),
                TimeUnit::Millisecond => downcast_and_iter!(column, TimestampMillisecondArray)
                    .map(|x| {
                        x.map(|ts| {
                            let ts = DateTime::from_timestamp_millis(ts).unwrap();
                            format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_millis())
                        })
                    })
                    .collect(),
                TimeUnit::Microsecond => downcast_and_iter!(column, TimestampMicrosecondArray)
                    .map(|x| {
                        x.map(|ts| {
                            let ts = DateTime::from_timestamp_micros(ts).unwrap();
                            format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
                        })
                    })
                    .collect(),
                TimeUnit::Nanosecond => downcast_and_iter!(column, TimestampNanosecondArray)
                    .map(|x| {
                        x.map(|ts| {
                            let ts = DateTime::from_timestamp_nanos(ts);
                            format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_nanos())
                        })
                    })
                    .collect(),
            };
            Arc::new(StringArray::from(timestamps)) as ArrayRef
        }
    }
}

#[allow(
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn convert_uint_to_int_datatypes(
    fields: &mut Vec<Field>,
    field: &Field,
    column: &ArrayRef,
    metadata: HashMap<String, String>,
    data_format: DataSerializationFormat,
    precision_scale: (i32, i32),
) -> ArrayRef {
    match data_format {
        DataSerializationFormat::Arrow => {
            match field.data_type() {
                DataType::UInt64 => {
                    fields.push(
                        Field::new(
                            field.name(),
                            DataType::Decimal128(precision_scale.0 as u8, precision_scale.1 as i8),
                            field.is_nullable(),
                        )
                        .with_metadata(metadata),
                    );
                    // converted_column
                    Arc::new(
                        Decimal128Array::from_unary(
                            column.as_any().downcast_ref::<UInt64Array>().unwrap(),
                            |x| x as i128,
                        )
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                    )
                }
                DataType::UInt32 => {
                    fields.push(
                        Field::new(field.name(), DataType::Int64, field.is_nullable())
                            .with_metadata(metadata),
                    );
                    // converted_column
                    Arc::new(Int64Array::from_unary(
                        column.as_any().downcast_ref::<UInt32Array>().unwrap(),
                        |x| x as i64,
                    ))
                }
                DataType::UInt16 => {
                    fields.push(
                        Field::new(field.name(), DataType::Int32, field.is_nullable())
                            .with_metadata(metadata),
                    );
                    // converted_column
                    Arc::new(Int32Array::from_unary(
                        column.as_any().downcast_ref::<UInt16Array>().unwrap(),
                        |x| x as i32,
                    ))
                }
                DataType::UInt8 => {
                    fields.push(
                        Field::new(field.name(), DataType::Int16, field.is_nullable())
                            .with_metadata(metadata),
                    );
                    // converted_column
                    Arc::new(Int16Array::from_unary(
                        column.as_any().downcast_ref::<UInt8Array>().unwrap(),
                        |x| x as i16,
                    ))
                }
                _ => {
                    fields.push(field.clone().with_metadata(metadata));
                    Arc::clone(column)
                }
            }
        }
        DataSerializationFormat::Json => {
            fields.push(field.clone().with_metadata(metadata));
            Arc::clone(column)
        }
    }
}

pub struct NormalizedIdent(pub Vec<Ident>);

impl From<&NormalizedIdent> for String {
    fn from(ident: &NormalizedIdent) -> Self {
        ident
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".")
    }
}

impl From<NormalizedIdent> for ObjectName {
    fn from(ident: NormalizedIdent) -> Self {
        Self(ident.0)
    }
}

impl std::fmt::Display for NormalizedIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::as_conversions, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float64Array, Int32Array, TimestampSecondArray, UInt64Array, UnionArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_first_non_empty_type() {
        let int_array = Int32Array::from(vec![Some(1), None, Some(34)]);
        let float_array = Float64Array::from(vec![None, Some(3.2), None]);
        let type_ids = [0_i8, 1, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let union_fields = [
            (0, Arc::new(Field::new("A", DataType::Int32, false))),
            (1, Arc::new(Field::new("B", DataType::Float64, false))),
        ]
        .into_iter()
        .collect();

        let children = vec![Arc::new(int_array) as ArrayRef, Arc::new(float_array)];

        let union_array = UnionArray::try_new(union_fields, type_ids, None, children)
            .expect("Failed to create UnionArray");

        let result = first_non_empty_type(&union_array);
        assert!(result.is_some());
        let (data_type, array) = result.unwrap();
        assert_eq!(data_type, DataType::Int32);
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn test_convert_timestamp_to_struct() {
        let cases = [
            (TimeUnit::Second, Some(1_627_846_261), "1627846261"),
            (
                TimeUnit::Millisecond,
                Some(1_627_846_261_233),
                "1627846261.233",
            ),
            (
                TimeUnit::Microsecond,
                Some(1_627_846_261_233_222),
                "1627846261.233222",
            ),
            (
                TimeUnit::Nanosecond,
                Some(1_627_846_261_233_222_111),
                "1627846261.233222111",
            ),
        ];
        for (unit, timestamp, expected) in &cases {
            let values = vec![*timestamp, None];
            let timestamp_array = match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)) as ArrayRef,
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from(values)) as ArrayRef
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(values)) as ArrayRef
                }
            };
            let result =
                convert_timestamp_to_struct(&timestamp_array, *unit, DataSerializationFormat::Json);
            let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(string_array.len(), 2);
            assert_eq!(string_array.value(0), *expected);
            assert!(string_array.is_null(1));
        }
    }

    #[test]
    fn test_convert_record_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
        ]));
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let timestamp_array = Arc::new(TimestampSecondArray::from(vec![
            Some(1_627_846_261),
            None,
            Some(1_627_846_262),
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![int_array, timestamp_array]).unwrap();
        let records = vec![batch];
        let (converted_batches, column_infos) =
            convert_record_batches(records.clone(), DataSerializationFormat::Json).unwrap();

        let converted_batch = &converted_batches[0];
        assert_eq!(converted_batches.len(), 1);
        assert_eq!(converted_batch.num_columns(), 2);
        assert_eq!(converted_batch.num_rows(), 3);

        let converted_timestamp_array = converted_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(converted_timestamp_array.value(0), "1627846261");
        assert!(converted_timestamp_array.is_null(1));
        assert_eq!(converted_timestamp_array.value(2), "1627846262");

        assert_eq!(column_infos[0].name, "int_col");
        assert_eq!(column_infos[0].r#type, "fixed");
        assert_eq!(column_infos[1].name, "timestamp_col");
        assert_eq!(column_infos[1].r#type, "timestamp_ntz");

        let (converted_batches, _) =
            convert_record_batches(records, DataSerializationFormat::Arrow).unwrap();
        let converted_batch = &converted_batches[0];
        let converted_timestamp_array = converted_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(converted_timestamp_array.value(0), 1_627_846_261);
        assert!(converted_timestamp_array.is_null(1));
        assert_eq!(converted_timestamp_array.value(2), 1_627_846_262);
    }

    #[allow(
        clippy::needless_pass_by_value,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    fn check_record_batches_uint_to_int(
        batches: Vec<RecordBatch>,
        converted_batches: Vec<RecordBatch>,
        column_infos: Vec<ColumnInfo>,
    ) -> i32 {
        assert_eq!(batches.len(), 1);
        assert_eq!(converted_batches.len(), 1);

        let batch = &batches[0];
        let converted_batch = &converted_batches[0];

        assert_eq!(converted_batch.num_columns(), batch.num_columns());
        assert_eq!(converted_batch.num_rows(), batch.num_rows());

        let mut fields_tested = 0;
        for (i, field) in batch.schema().fields.into_iter().enumerate() {
            let converted_field = converted_batch
                .schema_ref()
                .field_with_name(field.name())
                .unwrap();

            let column_info = &column_infos[i];
            let converted_column = &converted_batch.columns()[i];
            assert_eq!(column_infos[i].name, *converted_field.name());
            assert_eq!(
                column_infos[i].r#type.to_ascii_uppercase(),
                converted_field.metadata()["logicalType"]
            );
            // natively precision is u8, scale i8 but column_info is using i32
            let metadata_precision: i32 = converted_field.metadata()["precision"].parse().unwrap();
            let metadata_scale: i32 = converted_field.metadata()["scale"].parse().unwrap();
            assert_ne!(column_info.precision.unwrap(), 0);
            assert_eq!(column_info.precision.unwrap(), metadata_precision);
            assert_eq!(column_info.scale.unwrap(), metadata_scale);
            match field.data_type() {
                DataType::UInt64 => {
                    assert_eq!(
                        *converted_field.data_type(),
                        DataType::Decimal128(metadata_precision as u8, metadata_scale as i8)
                    );
                    let values: Decimal128Array = converted_column
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .into_iter()
                        .collect();
                    assert_eq!(
                        values,
                        Decimal128Array::from(vec![0, 1, i128::from(u64::MAX)])
                    );
                    fields_tested += 1;
                }
                DataType::UInt32 => {
                    assert_eq!(*converted_field.data_type(), DataType::Int64);
                    let values: Int64Array = converted_column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .into_iter()
                        .collect();
                    assert_eq!(values, Int64Array::from(vec![0, 1, i64::from(u32::MAX)]));
                    fields_tested += 1;
                }
                DataType::UInt16 => {
                    assert_eq!(*converted_field.data_type(), DataType::Int32);
                    let values: Int32Array = converted_column
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .into_iter()
                        .collect();
                    assert_eq!(values, Int32Array::from(vec![0, 1, i32::from(u16::MAX)]));
                    fields_tested += 1;
                }
                DataType::UInt8 => {
                    assert_eq!(*converted_field.data_type(), DataType::Int16);
                    let values: Int16Array = converted_column
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .unwrap()
                        .into_iter()
                        .collect();
                    assert_eq!(values, Int16Array::from(vec![0, 1, i16::from(u8::MAX)]));
                    fields_tested += 1;
                }
                _ => {
                    panic!("Bad DataType: {}", field.data_type());
                }
            };
        }
        fields_tested
    }

    #[test]
    fn test_convert_record_batches_uint() {
        let record_batches = vec![RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("row_num_uint64", DataType::UInt64, false),
                Field::new("row_num_uint32", DataType::UInt32, false),
                Field::new("row_num_uint16", DataType::UInt16, false),
                Field::new("row_num_uint8", DataType::UInt8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![0, 1, u64::MAX])),
                Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
                Arc::new(UInt16Array::from(vec![0, 1, u16::MAX])),
                Arc::new(UInt8Array::from(vec![0, 1, u8::MAX])),
            ],
        )
        .unwrap()];

        let (converted_batches, column_infos) =
            convert_record_batches(record_batches.clone(), DataSerializationFormat::Arrow).unwrap();

        let fields_tested =
            check_record_batches_uint_to_int(record_batches, converted_batches, column_infos);
        assert_eq!(fields_tested, 4);
    }
}
