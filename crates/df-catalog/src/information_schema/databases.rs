//! [`InformationSchemaDatabases`] that implements the SQL [Information Schema Databases] for Snowflake.
//!
//! [Information Schema Databases]: https://docs.snowflake.com/en/sql-reference/info-schema/databases

use crate::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    array::TimestampMillisecondBuilder,
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
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
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
            created_at: TimestampMillisecondBuilder::new(),
            updated_at: TimestampMillisecondBuilder::new(),
        }
    }
}

impl PartitionStream for InformationSchemaDatabases {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let result = self
            .config
            .make_databases(&mut builder)
            .and_then(|()| Ok(builder.finish()?));

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(vec![result]),
        ))
    }
}

pub struct InformationSchemaDatabasesBuilder {
    schema: SchemaRef,
    database_names: StringBuilder,
    database_owners: StringBuilder,
    database_types: StringBuilder,
    created_at: TimestampMillisecondBuilder,
    updated_at: TimestampMillisecondBuilder,
}

impl InformationSchemaDatabasesBuilder {
    pub fn add_database(
        &mut self,
        database_name: impl AsRef<str>,
        database_owner: impl AsRef<str>,
        database_type: impl AsRef<str>,
        created_at: Option<i64>,
        updated_at: Option<i64>,
    ) {
        self.database_names.append_value(database_name.as_ref());
        self.database_owners.append_value(database_owner.as_ref());
        self.database_types.append_value(database_type.as_ref());
        self.created_at.append_option(created_at);
        self.updated_at.append_option(updated_at);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.database_names.finish()),
                Arc::new(self.database_owners.finish()),
                Arc::new(self.database_types.finish()),
                Arc::new(self.created_at.finish()),
                Arc::new(self.updated_at.finish()),
            ],
        )
    }
}
