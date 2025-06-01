//! [`InformationSchemaDatabases`] that implements the SQL [Information Schema Databases] for Snowflake.
//!
//! [Information Schema Databases]: https://docs.snowflake.com/en/sql-reference/info-schema/databases

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
pub struct InformationSchemaDatabases {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaDatabases {
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("database_name", DataType::Utf8, false),
            Field::new("database_owner", DataType::Utf8, false),
            Field::new("database_type", DataType::Utf8, false),
        ]))
    }
    pub(crate) fn new(config: InformationSchemaConfig) -> Self {
        let schema = Self::schema();
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaDatabasesBuilder {
        InformationSchemaDatabasesBuilder {
            schema: Arc::clone(&self.schema),
            database_names: StringBuilder::new(),
            database_owners: StringBuilder::new(),
            database_types: StringBuilder::new(),
        }
    }
}

impl PartitionStream for InformationSchemaDatabases {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        self.config.make_databases(&mut builder);
        let result = builder
            .finish()
            .map_err(|e| DataFusionError::ArrowError(e, None));

        let stream = futures::stream::iter(vec![result]);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub struct InformationSchemaDatabasesBuilder {
    schema: SchemaRef,
    database_names: StringBuilder,
    database_owners: StringBuilder,
    database_types: StringBuilder,
}

impl InformationSchemaDatabasesBuilder {
    pub fn add_database(
        &mut self,
        database_name: impl AsRef<str>,
        database_owner: impl AsRef<str>,
        database_type: impl AsRef<str>,
    ) {
        self.database_names.append_value(database_name.as_ref());
        self.database_owners.append_value(database_owner.as_ref());
        self.database_types.append_value(database_type.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.database_names.finish()),
                Arc::new(self.database_owners.finish()),
                Arc::new(self.database_types.finish()),
            ],
        )
    }
}
