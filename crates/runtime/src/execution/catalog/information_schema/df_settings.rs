use crate::execution::catalog::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::config::ConfigEntry;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaDfSettings {
    schema: SchemaRef,
}

impl InformationSchemaDfSettings {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    fn builder(&self) -> InformationSchemaDfSettingsBuilder {
        InformationSchemaDfSettingsBuilder {
            names: StringBuilder::new(),
            values: StringBuilder::new(),
            descriptions: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaDfSettings {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        InformationSchemaConfig::make_df_settings(ctx.session_config().options(), &mut builder);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter([builder
                .finish()
                .map_err(|e| DataFusionError::ArrowError(e, None))]),
        ))
    }
}

pub struct InformationSchemaDfSettingsBuilder {
    schema: SchemaRef,
    names: StringBuilder,
    values: StringBuilder,
    descriptions: StringBuilder,
}

impl InformationSchemaDfSettingsBuilder {
    pub fn add_setting(&mut self, entry: ConfigEntry) {
        self.names.append_value(entry.key);
        self.values.append_option(entry.value);
        self.descriptions.append_value(entry.description);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.names.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.descriptions.finish()),
            ],
        )
    }
}
