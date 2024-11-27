use arrow::array::{Array, UnionArray};
use arrow::datatypes::{Field, Schema};
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


pub fn convert_record_batches(records: Vec<RecordBatch>) -> DataFusionResult<Vec<RecordBatch>> {
    let mut converted_batches = Vec::new();

    for batch in records {
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        for (i, column) in batch.columns().iter().enumerate() {
            let converted_column = if let Some(union_array) = column.as_any().downcast_ref::<UnionArray>() {
                if let Some((data_type, array)) = first_non_empty_type(union_array) {
                    fields.push(Field::new(batch.schema().field(i).name(), data_type, true));
                    array
                } else {
                    fields.push(batch.schema().field(i).clone());
                    Arc::clone(column)
                }
            } else {
                fields.push(batch.schema().field(i).clone());
                Arc::clone(column)
            };
            columns.push(converted_column);
        }
        let new_schema = Arc::new(Schema::new(fields));
        let converted_batch = RecordBatch::try_new(new_schema, columns)?;
        converted_batches.push(converted_batch);
    }

    Ok(converted_batches)
}