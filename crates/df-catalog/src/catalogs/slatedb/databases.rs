use crate::catalogs::slatedb::config::SlateDBViewConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct DatabasesView {
    schema: SchemaRef,
    config: SlateDBViewConfig,
}

impl DatabasesView {
    pub(crate) fn new(config: SlateDBViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("database_name", DataType::Utf8, false),
            Field::new("volume_name", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> DatabasesViewBuilder {
        DatabasesViewBuilder {
            database_names: StringBuilder::new(),
            volume_names: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for DatabasesView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_databases(&mut builder).await?;
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}

pub struct DatabasesViewBuilder {
    schema: SchemaRef,
    database_names: StringBuilder,
    volume_names: StringBuilder,
}

impl DatabasesViewBuilder {
    pub fn add_database(&mut self, database_name: impl AsRef<str>, volume_name: impl AsRef<str>) {
        // Note: append_value is actually infallible.
        self.database_names.append_value(database_name.as_ref());
        self.volume_names.append_value(volume_name.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.database_names.finish()),
                Arc::new(self.volume_names.finish()),
            ],
        )
    }
}
