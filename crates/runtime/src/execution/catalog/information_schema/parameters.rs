use crate::execution::catalog::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::array::builder::{BooleanBuilder, UInt64Builder, UInt8Builder};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Signature;
use datafusion_common::DataFusionError;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaParameters {
    schema: SchemaRef,
}

impl InformationSchemaParameters {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("parameter_mode", DataType::Utf8, false),
            Field::new("parameter_name", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("parameter_default", DataType::Utf8, true),
            Field::new("is_variadic", DataType::Boolean, false),
            // `rid` (short for `routine id`) is used to differentiate parameters from different signatures
            // (It serves as the group-by key when generating the `SHOW FUNCTIONS` query).
            // For example, the following signatures have different `rid` values:
            //     - `datetrunc(Utf8, Timestamp(Microsecond, Some("+TZ"))) -> Timestamp(Microsecond, Some("+TZ"))`
            //     - `datetrunc(Utf8View, Timestamp(Nanosecond, None)) -> Timestamp(Nanosecond, None)`
            Field::new("rid", DataType::UInt8, false),
        ]));

        Self { schema }
    }

    fn builder(&self) -> InformationSchemaParametersBuilder {
        InformationSchemaParametersBuilder {
            schema: Arc::clone(&self.schema),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            ordinal_position: UInt64Builder::new(),
            parameter_mode: StringBuilder::new(),
            parameter_name: StringBuilder::new(),
            data_type: StringBuilder::new(),
            parameter_default: StringBuilder::new(),
            is_variadic: BooleanBuilder::new(),
            rid: UInt8Builder::new(),
        }
    }
}

impl PartitionStream for InformationSchemaParameters {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let result = InformationSchemaConfig::make_parameters(
            ctx.scalar_functions(),
            ctx.aggregate_functions(),
            ctx.window_functions(),
            ctx.session_config().options(),
            &mut builder,
        )
        .and_then(|()| {
            builder
                .finish()
                .map_err(|e| DataFusionError::ArrowError(e, None))
        });

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter([result]),
        ))
    }
}

pub struct InformationSchemaParametersBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    ordinal_position: UInt64Builder,
    parameter_mode: StringBuilder,
    parameter_name: StringBuilder,
    data_type: StringBuilder,
    parameter_default: StringBuilder,
    is_variadic: BooleanBuilder,
    rid: UInt8Builder,
}

impl InformationSchemaParametersBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn add_parameter(
        &mut self,
        specific_catalog: impl AsRef<str>,
        specific_schema: impl AsRef<str>,
        specific_name: impl AsRef<str>,
        ordinal_position: u64,
        parameter_mode: impl AsRef<str>,
        parameter_name: Option<String>,
        data_type: impl AsRef<str>,
        parameter_default: Option<String>,
        is_variadic: bool,
        rid: u8,
    ) {
        self.specific_catalog
            .append_value(specific_catalog.as_ref());
        self.specific_schema.append_value(specific_schema.as_ref());
        self.specific_name.append_value(specific_name.as_ref());
        self.ordinal_position.append_value(ordinal_position);
        self.parameter_mode.append_value(parameter_mode.as_ref());
        self.parameter_name.append_option(parameter_name);
        self.data_type.append_value(data_type.as_ref());
        self.parameter_default.append_option(parameter_default);
        self.is_variadic.append_value(is_variadic);
        self.rid.append_value(rid);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.ordinal_position.finish()),
                Arc::new(self.parameter_mode.finish()),
                Arc::new(self.parameter_name.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.parameter_default.finish()),
                Arc::new(self.is_variadic.finish()),
                Arc::new(self.rid.finish()),
            ],
        )
    }
}

pub fn get_udf_args_and_return_types(udf: &Arc<ScalarUDF>) -> Vec<(Vec<String>, Option<String>)> {
    get_args_and_return_types(udf.signature(), |arg_types| {
        udf.return_type(arg_types).ok().map(|t| t.to_string())
    })
}

pub fn get_udaf_args_and_return_types(
    udaf: &Arc<AggregateUDF>,
) -> Vec<(Vec<String>, Option<String>)> {
    get_args_and_return_types(udaf.signature(), |arg_types| {
        udaf.return_type(arg_types).ok().map(|t| t.to_string())
    })
}

pub fn get_udwf_args_and_return_types(udwf: &Arc<WindowUDF>) -> Vec<(Vec<String>, Option<String>)> {
    get_args_and_return_types(udwf.signature(), |_| None)
}

fn get_args_and_return_types<F>(
    signature: &Signature,
    get_return_type: F,
) -> Vec<(Vec<String>, Option<String>)>
where
    F: Fn(&[DataType]) -> Option<String>,
{
    let arg_type_combinations = signature.type_signature.get_example_types();

    if arg_type_combinations.is_empty() {
        vec![(vec![], None)]
    } else {
        arg_type_combinations
            .into_iter()
            .map(|arg_types| {
                let return_type = get_return_type(&arg_types);
                let arg_types = arg_types
                    .into_iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>();
                (arg_types, return_type)
            })
            .collect()
    }
}
