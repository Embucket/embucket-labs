pub mod current_database;
pub mod current_schema;
pub mod current_version;
mod last_query_id;

use crate::session_params::SessionParams;
use datafusion::arrow::array::ListArray;
use datafusion::arrow::datatypes::{DataType, Field};
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
        create_udf($name, vec![], DataType::Utf8, Volatility::Volatile, fun)
    }};
}

/// Returns active search path schemas.
fn current_schemas_udf() -> ScalarUDF {
    let fun: ScalarFunctionImplementation = Arc::new(move |_args| {
        let list_array = ListArray::new_null(Arc::new(Field::new("item", DataType::Utf8, true)), 1);
        Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
            list_array,
        ))))
    });
    create_udf(
        "current_schemas",
        vec![],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Volatility::Volatile,
        fun,
    )
}

/// Returns the name of the warehouse in use for the current session.
fn current_warehouse_udf() -> ScalarUDF {
    create_session_context_udf!("current_warehouse", "default")
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

/// Returns a unique system identifier for the Embucket session corresponding to the present connection.
fn current_session_udf() -> ScalarUDF {
    create_session_context_udf!("current_session", "default")
}

/// Returns the IP address of the client that submitted the request.
fn current_ip_address_udf() -> ScalarUDF {
    create_session_context_udf!("current_ip_address", "")
}

pub fn register_session_context_udfs(
    registry: &mut dyn FunctionRegistry,
    session_params: &Arc<SessionParams>,
) -> Result<()> {
    let udfs = [
        current_client_udf(),
        current_ip_address_udf(),
        current_role_udf(),
        current_role_type_udf(),
        current_schemas_udf(),
        current_session_udf(),
        current_warehouse_udf(),
    ];

    for udf in udfs {
        registry.register_udf(udf.into())?;
    }

    let functions: Vec<Arc<ScalarUDF>> = vec![
        last_query_id::get_udf(),
        Arc::new(ScalarUDF::from(current_version::CurrentVersion::new(
            session_params.clone(),
        ))),
        Arc::new(ScalarUDF::from(current_database::CurrentDatabase::new(
            session_params.clone(),
        ))),
        Arc::new(ScalarUDF::from(current_schema::CurrentSchema::new(
            session_params.clone(),
        ))),
    ];
    for func in functions {
        registry.register_udf(func)?;
    }
    Ok(())
}

#[must_use]
pub fn session_prop(property: &str) -> String {
    format!("embucket.session.{property}")
}
