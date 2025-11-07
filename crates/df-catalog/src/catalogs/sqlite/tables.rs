use crate::catalogs::sqlite::metastore_config::MetastoreViewConfig;
use datafusion::arrow::array::Int64Builder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
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
pub struct TablesView {
    schema: SchemaRef,
    config: MetastoreViewConfig,
}

impl TablesView {
    pub(crate) fn new(config: MetastoreViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("database_name", DataType::Utf8, false),
            Field::new("volume_name", DataType::Utf8, true),
            Field::new("owner", DataType::Utf8, true),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_format", DataType::Utf8, false),
            Field::new("total_bytes", DataType::Int64, false),
            Field::new("total_rows", DataType::Int64, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> TablesViewBuilder {
        TablesViewBuilder {
            table_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            database_names: StringBuilder::new(),
            volume_names: StringBuilder::new(),
            owners: StringBuilder::new(),
            table_types: StringBuilder::new(),
            table_format_values: StringBuilder::new(),
            total_bytes_values: Int64Builder::new(),
            total_rows_values: Int64Builder::new(),
            created_at_timestamps: StringBuilder::new(),
            updated_at_timestamps: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for TablesView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_tables(&mut builder).await?;
                Ok(builder.finish()?)
            }),
        ))
    }
}

pub struct TablesViewBuilder {
    schema: SchemaRef,
    table_names: StringBuilder,
    schema_names: StringBuilder,
    database_names: StringBuilder,
    volume_names: StringBuilder,
    owners: StringBuilder,
    table_types: StringBuilder,
    table_format_values: StringBuilder,
    total_bytes_values: Int64Builder,
    total_rows_values: Int64Builder,
    created_at_timestamps: StringBuilder,
    updated_at_timestamps: StringBuilder,
}

impl TablesViewBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn add_tables(
        &mut self,
        table_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        volume_name: Option<impl AsRef<str>>,
        owner: Option<impl AsRef<str>>,
        table_type: impl AsRef<str>,
        table_format: impl AsRef<str>,
        total_files_size: i64,
        total_rows: i64,
        created_at: impl AsRef<str>,
        updated_at: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallible.
        self.table_names.append_value(table_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.volume_names.append_option(volume_name);
        self.owners.append_option(owner);
        self.table_types.append_value(table_type);
        self.table_format_values.append_value(table_format.as_ref());
        self.total_bytes_values.append_value(total_files_size);
        self.total_rows_values.append_value(total_rows);
        self.created_at_timestamps.append_value(created_at.as_ref());
        self.updated_at_timestamps.append_value(updated_at.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.table_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.database_names.finish()),
                Arc::new(self.volume_names.finish()),
                Arc::new(self.owners.finish()),
                Arc::new(self.table_types.finish()),
                Arc::new(self.table_format_values.finish()),
                Arc::new(self.total_bytes_values.finish()),
                Arc::new(self.total_rows_values.finish()),
                Arc::new(self.created_at_timestamps.finish()),
                Arc::new(self.updated_at_timestamps.finish()),
            ],
        )
    }
}
