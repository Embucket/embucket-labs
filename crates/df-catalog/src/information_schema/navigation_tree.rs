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
pub struct InformationSchemaNavigationTree {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaNavigationTree {
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("database", DataType::Utf8, false),
            Field::new("schema", DataType::Utf8, true),
            Field::new("table", DataType::Utf8, true),
            Field::new("table_type", DataType::Utf8, true),
        ]))
    }
    pub(crate) fn new(config: InformationSchemaConfig) -> Self {
        let schema = Self::schema();
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaNavigationTreeBuilder {
        InformationSchemaNavigationTreeBuilder {
            databases: StringBuilder::new(),
            schemas: StringBuilder::new(),
            tables: StringBuilder::new(),
            table_types: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaNavigationTree {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let result = self
            .config
            .make_navigation_tree(&mut builder)
            .and_then(|()| {
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            });
        let stream = futures::stream::iter(vec![result]);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub struct InformationSchemaNavigationTreeBuilder {
    schema: SchemaRef,
    databases: StringBuilder,
    schemas: StringBuilder,
    tables: StringBuilder,
    table_types: StringBuilder,
}

impl InformationSchemaNavigationTreeBuilder {
    pub fn add_navigation_tree(
        &mut self,
        database: impl AsRef<str>,
        schema: Option<String>,
        table: Option<String>,
        table_type: Option<TableType>,
    ) {
        // Note: append_value is actually infallible.
        self.databases.append_value(database.as_ref());
        self.schemas.append_option(schema);
        self.tables.append_option(table);
        self.table_types
            .append_option(table_type.map(|ttype| match ttype {
                TableType::Base => "TABLE",
                TableType::View => "VIEW",
                TableType::Temporary => "TEMPORARY",
            }));
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.databases.finish()),
                Arc::new(self.schemas.finish()),
                Arc::new(self.tables.finish()),
                Arc::new(self.table_types.finish()),
            ],
        )
    }
}
