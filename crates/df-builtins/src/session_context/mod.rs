use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Volatility, create_udf,
};
use std::sync::Arc;

macro_rules! create_session_context_udf {
    ($name:expr) => {{
        let fun: ScalarFunctionImplementation =
            Arc::new(|_args| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))));
        create_udf($name, vec![], DataType::Utf8, Volatility::Immutable, fun)
    }};
}

fn current_database_udf() -> ScalarUDF {
    create_session_context_udf!("current_database")
}

fn current_schema_udf() -> ScalarUDF {
    create_session_context_udf!("current_schema")
}

fn current_warehouse_udf() -> ScalarUDF {
    create_session_context_udf!("current_warehouse")
}

pub fn register_session_context_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(current_database_udf().into())?;
    registry.register_udf(current_schema_udf().into())?;
    registry.register_udf(current_warehouse_udf().into())?;
    Ok(())
}
