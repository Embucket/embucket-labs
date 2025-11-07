use crate::catalogs::slatedb::metastore_config::MetastoreViewConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::Int64Builder,
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct SchemasView {
    schema: SchemaRef,
    config: MetastoreViewConfig,
}

impl SchemasView {
    pub(crate) fn new(config: MetastoreViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_id", DataType::Int64, false),
            Field::new("database_id", DataType::Int64, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("database_name", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> SchemasViewBuilder {
        SchemasViewBuilder {
            schema_ids: Int64Builder::new(),
            database_ids: Int64Builder::new(),
            schema_names: StringBuilder::new(),
            database_names: StringBuilder::new(),
            created_at_timestamps: StringBuilder::new(),
            updated_at_timestamps: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for SchemasView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_schemas(&mut builder).await?;
                Ok(builder.finish()?)
            }),
        ))
    }
}

pub struct SchemasViewBuilder {
    schema: SchemaRef,
    schema_ids: Int64Builder,
    database_ids: Int64Builder,
    schema_names: StringBuilder,
    database_names: StringBuilder,
    created_at_timestamps: StringBuilder,
    updated_at_timestamps: StringBuilder,
}

impl SchemasViewBuilder {
    pub fn add_schema(
        &mut self,
        schema_id: i64,
        database_id: i64,
        schema_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        created_at: impl AsRef<str>,
        updated_at: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallible.
        self.schema_ids.append_value(schema_id);
        self.database_ids.append_value(database_id);
        self.schema_names.append_value(schema_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.created_at_timestamps.append_value(created_at.as_ref());
        self.updated_at_timestamps.append_value(updated_at.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.schema_ids.finish()),
                Arc::new(self.database_ids.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.database_names.finish()),
                Arc::new(self.created_at_timestamps.finish()),
                Arc::new(self.updated_at_timestamps.finish()),
            ],
        )
    }
}
