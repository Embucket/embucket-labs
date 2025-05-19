#[allow(clippy::module_inception)]
pub mod catalog;
pub mod catalog_list;
pub mod catalogs;
pub mod error;
pub mod schema;
pub mod table;

pub mod information_schema;
#[cfg(test)]
pub mod tests;

pub mod test_utils {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::compute::{
        SortColumn, SortOptions, lexsort_to_indices, take_record_batch,
    };
    use datafusion::arrow::datatypes::DataType;

    #[allow(clippy::unwrap_used, clippy::must_use_candidate)]
    pub fn sort_record_batch_by_sortable_columns(batch: &RecordBatch) -> RecordBatch {
        let sort_columns: Vec<SortColumn> = (0..batch.num_columns())
            .filter_map(|i| {
                let col = batch.column(i).clone();
                let field = batch.schema().field(i).clone();
                if matches!(field.data_type(), DataType::Null) {
                    None
                } else {
                    Some(SortColumn {
                        values: col,
                        options: Some(SortOptions::default()),
                    })
                }
            })
            .collect();

        if sort_columns.is_empty() {
            return batch.clone();
        }

        let indices = lexsort_to_indices(&sort_columns, Some(batch.num_rows())).unwrap();
        take_record_batch(batch, &indices).unwrap()
    }
}
