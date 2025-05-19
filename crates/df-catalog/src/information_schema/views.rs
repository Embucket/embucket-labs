//! [`InformationSchemaViews`] that implements the SQL [Information Schema Views] for Snowflake.
//!
//! [Information Schema Views]: https://docs.snowflake.com/en/sql-reference/info-schema/views

use crate::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_expr::TableType;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaViews {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaViews {
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("view_catalog", DataType::Utf8, false),
            Field::new("view_schema", DataType::Utf8, false),
            Field::new("view_name", DataType::Utf8, false),
            Field::new("view_type", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, true),
        ]))
    }
    pub fn new(config: InformationSchemaConfig) -> Self {
        let schema = Self::schema();
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaViewBuilder {
        InformationSchemaViewBuilder {
            view_catalogs: StringBuilder::new(),
            view_schemas: StringBuilder::new(),
            view_names: StringBuilder::new(),
            view_types: StringBuilder::new(),
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
    view_catalogs: StringBuilder,
    view_schemas: StringBuilder,
    view_names: StringBuilder,
    view_types: StringBuilder,
    definitions: StringBuilder,
}

impl InformationSchemaViewBuilder {
    pub(crate) fn add_view(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        view_name: impl AsRef<str>,
        view_type: TableType,
        definition: Option<&str>,
    ) {
        // Note: append_value is actually infallible.
        self.view_catalogs.append_value(catalog_name.as_ref());
        self.view_schemas.append_value(schema_name.as_ref());
        self.view_names.append_value(view_name.as_ref());
        self.view_types.append_value(match view_type {
            TableType::Base => "TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "TEMPORARY",
        });
        self.definitions.append_option(definition.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.view_catalogs.finish()),
                Arc::new(self.view_schemas.finish()),
                Arc::new(self.view_names.finish()),
                Arc::new(self.view_types.finish()),
                Arc::new(self.definitions.finish()),
            ],
        )
    }
}
