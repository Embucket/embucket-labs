use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Volatility, create_udf,
};
use std::sync::Arc;

macro_rules! create_session_context_udf {
    ($name:expr, $default_value:expr) => {{
        let value = $default_value.to_string();
        let fun: ScalarFunctionImplementation = Arc::new(move |_args| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                value.clone(),
            ))))
        });
        create_udf($name, vec![], DataType::Utf8, Volatility::Immutable, fun)
    }};
}

/// Returns the name of the current database, which varies depending on where you call the function
fn current_database_udf() -> ScalarUDF {
    create_session_context_udf!("current_database", "default")
}

/// Returns the name of the current schema, which varies depending on where you call the function
fn current_schema_udf() -> ScalarUDF {
    create_session_context_udf!("current_schema", "default")
}

/// Returns the name of the warehouse in use for the current session.
fn current_warehouse_udf() -> ScalarUDF {
    create_session_context_udf!("current_warehouse", "default")
}

/// Returns the current Embucket version.
fn current_version_udf() -> ScalarUDF {
    create_session_context_udf!("current_version", env!("CARGO_PKG_VERSION"))
}

/// Returns the version of the client from which the function was called.
fn current_client_udf() -> ScalarUDF {
    let version = format!("Embucket {}", env!("CARGO_PKG_VERSION"));
    create_session_context_udf!("current_client", version)
}

/// Calling the `CURRENT_ROLE_TYPE` function returns ROLE if the current active (primary) role
/// in the session is an account role.
fn current_role_type_udf() -> ScalarUDF {
    create_session_context_udf!("current_role_type", "ROLE")
}

/// Returns the name of the primary role in use for the current session when the primary role
/// is an account-level role or NULL if the role in use for the current session is a database role.
fn current_role_udf() -> ScalarUDF {
    create_session_context_udf!("current_role", "default")
}

pub fn register_session_context_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(current_database_udf().into())?;
    registry.register_udf(current_schema_udf().into())?;
    registry.register_udf(current_warehouse_udf().into())?;
    registry.register_udf(current_version_udf().into())?;
    registry.register_udf(current_client_udf().into())?;
    registry.register_udf(current_role_type_udf().into())?;
    registry.register_udf(current_role_udf().into())?;
    Ok(())
}
