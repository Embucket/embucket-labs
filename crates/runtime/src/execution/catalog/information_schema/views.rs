//! [`InformationSchemaViews`] that implements the SQL [Information Schema Views] for Snowflake.
//!
//! [Information Schema Views]: https://docs.snowflake.com/en/sql-reference/info-schema/views

use crate::execution::catalog::information_schema::config::InformationSchemaConfig;
use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_schema::ArrowError;
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaViews {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaViews {
    pub fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaViewBuilder {
        InformationSchemaViewBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            definitions: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaViews {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_views(&mut builder).await?;
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}

pub struct InformationSchemaViewBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    definitions: StringBuilder,
}

impl InformationSchemaViewBuilder {
    pub(crate) fn add_view(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        definition: Option<&str>,
    ) {
        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.definitions.append_option(definition.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.definitions.finish()),
            ],
        )
    }
}
