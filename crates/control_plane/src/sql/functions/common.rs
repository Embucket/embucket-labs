use crate::models::ColumnInfo;
use arrow::array::{
    Array, Int32Array, Int64Array, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UnionArray,
};
use arrow::datatypes::{Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use std::sync::Arc;

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
        println!("new schema: {:?}", new_schema);
        println!("columns: {:?}", columns);
        let converted_batch = RecordBatch::try_new(new_schema, columns)?;
        converted_batches.push(converted_batch);
    }

    Ok((converted_batches.clone(), column_infos))
}

fn convert_timestamp_to_struct(column: &ArrayRef, unit: &TimeUnit) -> ArrayRef {
    let (epoch, fraction) = match unit {
        TimeUnit::Second => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let epoch: Int64Array = array.clone().unary(|x| x);
            let fraction: Int32Array = Int32Array::from(vec![0; column.len()]);
            (epoch, fraction)
        }
        TimeUnit::Millisecond => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let epoch: Int64Array = array.clone().unary(|x| x / 1_000);
            let fraction: Int32Array = array.clone().unary(|x| (x % 1_000 * 1_000_000) as i32);
            (epoch, fraction)
        }
        TimeUnit::Microsecond => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let epoch: Int64Array = array.clone().unary(|x| x / 1_000_000);
            let fraction: Int32Array = array.clone().unary(|x| (x % 1_000_000 * 1_000) as i32);
            (epoch, fraction)
        }
        TimeUnit::Nanosecond => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let epoch: Int64Array = array.clone().unary(|x| x / 1_000_000_000);
            let fraction: Int32Array = array.clone().unary(|x| (x % 1_000_000_000) as i32);
            (epoch, fraction)
        }
    };

    // let timezone = Int32Array::from(vec![1440; column.len()]); // Assuming UTC timezone
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("epoch", DataType::Int64, false)),
            Arc::new(epoch) as ArrayRef,
        ),
        (
            Arc::new(Field::new("fraction", DataType::Int32, false)),
            Arc::new(fraction) as ArrayRef,
        ),
        // (
        //     Arc::new(Field::new("timezone", DataType::Int32, false)),
        //     Arc::new(timezone) as ArrayRef,
        // ),
    ]);

    Arc::new(struct_array) as ArrayRef
}
