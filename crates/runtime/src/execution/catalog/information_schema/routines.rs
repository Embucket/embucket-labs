use crate::execution::catalog::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaRoutines {
    schema: SchemaRef,
}

impl InformationSchemaRoutines {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("routine_type", DataType::Utf8, false),
            Field::new("is_deterministic", DataType::Boolean, true),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("function_type", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("syntax_example", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    fn builder(&self) -> InformationSchemaRoutinesBuilder {
        InformationSchemaRoutinesBuilder {
            schema: Arc::clone(&self.schema),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            routine_catalog: StringBuilder::new(),
            routine_schema: StringBuilder::new(),
            routine_name: StringBuilder::new(),
            routine_type: StringBuilder::new(),
            is_deterministic: BooleanBuilder::new(),
            data_type: StringBuilder::new(),
            function_type: StringBuilder::new(),
            description: StringBuilder::new(),
            syntax_example: StringBuilder::new(),
        }
    }
}

impl PartitionStream for InformationSchemaRoutines {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        InformationSchemaConfig::make_routines(
            ctx.scalar_functions(),
            ctx.aggregate_functions(),
            ctx.window_functions(),
            ctx.session_config().options(),
            &mut builder,
        );

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter([builder
                .finish()
                .map_err(|e| DataFusionError::ArrowError(e, None))]),
        ))
    }
}

pub struct InformationSchemaRoutinesBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    routine_catalog: StringBuilder,
    routine_schema: StringBuilder,
    routine_name: StringBuilder,
    routine_type: StringBuilder,
    is_deterministic: BooleanBuilder,
    data_type: StringBuilder,
    function_type: StringBuilder,
    description: StringBuilder,
    syntax_example: StringBuilder,
}

impl InformationSchemaRoutinesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn add_routine(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        routine_name: impl AsRef<str>,
        routine_type: impl AsRef<str>,
        is_deterministic: bool,
        data_type: Option<String>,
        function_type: impl AsRef<str>,
        description: Option<String>,
        syntax_example: Option<String>,
    ) {
        self.specific_catalog.append_value(catalog_name.as_ref());
        self.specific_schema.append_value(schema_name.as_ref());
        self.specific_name.append_value(routine_name.as_ref());
        self.routine_catalog.append_value(catalog_name.as_ref());
        self.routine_schema.append_value(schema_name.as_ref());
        self.routine_name.append_value(routine_name.as_ref());
        self.routine_type.append_value(routine_type.as_ref());
        self.is_deterministic.append_value(is_deterministic);
        self.data_type.append_option(data_type);
        self.function_type.append_value(function_type.as_ref());
        self.description.append_option(description);
        self.syntax_example.append_option(syntax_example);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.routine_catalog.finish()),
                Arc::new(self.routine_schema.finish()),
                Arc::new(self.routine_name.finish()),
                Arc::new(self.routine_type.finish()),
                Arc::new(self.is_deterministic.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.function_type.finish()),
                Arc::new(self.description.finish()),
                Arc::new(self.syntax_example.finish()),
            ],
        )
    }
}
