use crate::models::ColumnInfo;
use arrow::array::{
    Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UnionArray,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use std::sync::Arc;

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d-%H:%M:%S%.9f";

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
    let column_infos = ColumnInfo::from_batch(records.clone());

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
                    let converted_column = convert_timestamp_to_struct(column, unit);
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
        let converted_batch = RecordBatch::try_new(new_schema, columns)?;
        converted_batches.push(converted_batch);
    }

    Ok((converted_batches.clone(), column_infos))
}

fn convert_timestamp_to_struct(column: &ArrayRef, unit: &TimeUnit) -> ArrayRef {
    let now = Utc::now();
    let timestamps: Vec<_> = match unit {
        TimeUnit::Second => column
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                let ts = DateTime::from_timestamp(x.unwrap_or(now.timestamp()), 0).unwrap();
                format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
            })
            .collect(),
        TimeUnit::Millisecond => column
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                let ts =
                    DateTime::from_timestamp_millis(x.unwrap_or(now.timestamp_millis())).unwrap();
                format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
            })
            .collect(),
        TimeUnit::Microsecond => column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                let ts =
                    DateTime::from_timestamp_micros(x.unwrap_or(now.timestamp_micros())).unwrap();
                format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
            })
            .collect(),
        TimeUnit::Nanosecond => column
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .iter()
            .map(|x| {
                let ts =
                    DateTime::from_timestamp_nanos(x.unwrap_or(now.timestamp_nanos_opt().unwrap()));
                format!("{}.{}", ts.timestamp(), ts.timestamp_subsec_micros())
            })
            .collect(),
    };
    // let (epoch, fraction) = match unit {
    //     TimeUnit::Second => {
    //         let array = column
    //             .as_any()
    //             .downcast_ref::<TimestampSecondArray>()
    //             .unwrap();
    //         let epoch: Int64Array = array.iter().map(|x| x.unwrap_or(now)).collect();
    //         let fraction: Int32Array = Int32Array::from(vec![0; column.len()]);
    //         (epoch, fraction)
    //     }
    //     TimeUnit::Millisecond => {
    //         let array = column
    //             .as_any()
    //             .downcast_ref::<TimestampMillisecondArray>()
    //             .unwrap();
    //         let now_millis = now * 1_000;
    //         let epoch: Int64Array = array.iter().map(|x| x.unwrap_or(now_millis) / 1_000).collect();
    //         let fraction: Int32Array = array
    //             .iter()
    //             .map(|x| (x.unwrap_or(0) % 1_000 * 1_000_000) as i32)
    //             .collect();
    //         (epoch, fraction)
    //     }
    //     TimeUnit::Microsecond => {
    //         let array = column
    //             .as_any()
    //             .downcast_ref::<TimestampMicrosecondArray>()
    //             .unwrap();
    //         let now_micros = now * 1_000_000;
    //         let epoch: Int64Array = array.iter().map(|x| x.unwrap_or(now_micros) / 1_000_000).collect();
    //         let fraction: Int32Array = array
    //             .iter()
    //             .map(|x| (x.unwrap_or(0) % 1_000_000 * 1_000) as i32)
    //             .collect();
    //         (epoch, fraction)
    //     }
    //     TimeUnit::Nanosecond => {
    //         let array = column
    //             .as_any()
    //             .downcast_ref::<TimestampNanosecondArray>()
    //             .unwrap();
    //         let now_nanos = now * 1_000_000_000;
    //         let epoch: Int64Array = array.iter().map(|x| x.unwrap_or(now_nanos) / 1_000_000_000).collect();
    //         let fraction: Int32Array = array
    //             .iter()
    //             .map(|x| (x.unwrap_or(0) % 1_000_000_000) as i32)
    //             .collect();
    //         (epoch, fraction)
    //     }
    // };
    // let string_values: Vec<_> = epoch.iter().map(|x| x.unwrap_or(0).to_string()).collect();
    // let string_array = StringArray::from(string_values);
    // let timezone = Int32Array::from(vec![1440; column.len()]); // Assuming UTC timezone
    // let struct_array = StructArray::try_new(
    //     vec![
    //         Arc::new(Field::new("epoch", DataType::Int64, false)),
    //         Arc::new(Field::new("fraction", DataType::Int32, false)),
    //     ]
    //         .into(),
    //     vec![Arc::new(epoch) as ArrayRef, Arc::new(fraction) as ArrayRef],
    //     None,
    // )
    //     .unwrap();
    // Arc::new(epoch) as ArrayRef

    Arc::new(StringArray::from(timestamps)) as ArrayRef
}
