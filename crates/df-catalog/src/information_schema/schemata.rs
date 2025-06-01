//! [`InformationSchemata`] that implements the SQL [Information Schema Schemata] for Snowflake.
//!
//! [Information Schema Schemata]: https://docs.snowflake.com/en/sql-reference/info-schema/schemata

use crate::information_schema::config::InformationSchemaConfig;
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
pub struct InformationSchemata {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemata {
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("schema_owner", DataType::Utf8, true),
            Field::new("default_character_set_catalog", DataType::Utf8, true),
            Field::new("default_character_set_schema", DataType::Utf8, true),
            Field::new("default_character_set_name", DataType::Utf8, true),
            Field::new("sql_path", DataType::Utf8, true),
        ]))
    }
    pub fn new(config: InformationSchemaConfig) -> Self {
        let schema = Self::schema();
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemataBuilder {
        InformationSchemataBuilder {
            schema: Arc::clone(&self.schema),
            catalog_name: StringBuilder::new(),
            schema_name: StringBuilder::new(),
            schema_owner: StringBuilder::new(),
            default_character_set_catalog: StringBuilder::new(),
            default_character_set_schema: StringBuilder::new(),
            default_character_set_name: StringBuilder::new(),
            sql_path: StringBuilder::new(),
        }
    }
}

pub struct InformationSchemataBuilder {
    schema: SchemaRef,
    catalog_name: StringBuilder,
    schema_name: StringBuilder,
    schema_owner: StringBuilder,
    default_character_set_catalog: StringBuilder,
    default_character_set_schema: StringBuilder,
    default_character_set_name: StringBuilder,
    sql_path: StringBuilder,
}

impl InformationSchemataBuilder {
    pub(crate) fn add_schemata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        schema_owner: Option<&str>,
    ) {
        self.catalog_name.append_value(catalog_name);
        self.schema_name.append_value(schema_name);
        match schema_owner {
            Some(owner) => self.schema_owner.append_value(owner),
            None => self.schema_owner.append_null(),
        }
        self.default_character_set_catalog.append_null();
        self.default_character_set_schema.append_null();
        self.default_character_set_name.append_null();
        self.sql_path.append_null();
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_name.finish()),
                Arc::new(self.schema_name.finish()),
                Arc::new(self.schema_owner.finish()),
                Arc::new(self.default_character_set_catalog.finish()),
                Arc::new(self.default_character_set_schema.finish()),
                Arc::new(self.default_character_set_name.finish()),
                Arc::new(self.sql_path.finish()),
            ],
        )
    }
}

impl PartitionStream for InformationSchemata {
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
                config.make_schemata(&mut builder);
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}
