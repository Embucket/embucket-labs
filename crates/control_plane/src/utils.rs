use crate::models::ColumnInfo;
use arrow::array::{
    Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UnionArray,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use std::sync::Arc;

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
                            fields.push(field.clone().with_metadata(metadata));
                            Arc::clone(column)
                        }
                    } else {
                        fields.push(field.clone().with_metadata(metadata));
                        Arc::clone(column)
                    }
                }
                DataType::Timestamp(unit, _) => {
                    let converted_column = convert_timestamp_to_struct(column, *unit);
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
                _ => {
                    fields.push(field.clone().with_metadata(metadata));
                    Arc::clone(column)
                }
            };
            columns.push(converted_column);
        }
        let new_schema = Arc::new(Schema::new(fields));
        //println!("new schema: {:?}", new_schema);
        //println!("columns: {:?}", columns);
        let converted_batch = RecordBatch::try_new(new_schema, columns)?;
        converted_batches.push(converted_batch);
    }

    Ok((converted_batches.clone(), column_infos))
}

#[allow(
    clippy::unwrap_used,
    clippy::as_conversions,
    clippy::cast_possible_truncation
)]
fn convert_timestamp_to_struct(column: &ArrayRef, unit: TimeUnit) -> ArrayRef {
    let timestamps: Vec<_> = match unit {
        TimeUnit::Second => column
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                x.map(|ts| {
                    let ts = DateTime::from_timestamp(ts, 0).unwrap();
                    format!("{}", ts.timestamp())
                })
            })
            .collect(),
        TimeUnit::Millisecond => column
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                x.map(|ts| {
                    let ts = DateTime::from_timestamp_millis(ts).unwrap();
                    format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_millis())
                })
            })
            .collect(),
        TimeUnit::Microsecond => column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                x.map(|ts| {
                    let ts = DateTime::from_timestamp_micros(ts).unwrap();
                    format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
                })
            })
            .collect(),
        TimeUnit::Nanosecond => column
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .iter()
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::as_conversions, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, TimestampSecondArray, UnionArray};
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
            let result = convert_timestamp_to_struct(&timestamp_array, *unit);
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
        let (converted_batches, column_infos) = convert_record_batches(records).unwrap();

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
    }
}
