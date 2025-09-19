pub use df_catalog as catalog;
use std::sync::Arc;
pub mod datafusion;
pub mod dedicated_executor;
pub mod error;
pub mod error_code;
pub mod models;
pub mod query;
pub mod running_queries;
pub mod service;
pub mod session;
pub mod snowflake_error;
pub mod utils;

#[cfg(test)]
pub mod tests;

use crate::service::ExecutionService;
pub use error::{Error, Result};
pub use running_queries::AbortQuery;
pub use snowflake_error::SnowflakeError;

use ::datafusion::arrow::array::Int64Array;
use ::datafusion::arrow::datatypes::DataType;
use ::datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use ::datafusion_common::ScalarValue;
use ::datafusion_expr::{ScalarFunctionImplementation, create_udf};
use datafusion_common::DataFusionError;
use std::thread;
use std::time::Duration;

pub trait ExecutionAppState {
    fn get_execution_svc(&self) -> Arc<dyn ExecutionService>;
}

/// Returns a custom scalar UDF named `sleep`, which simulates a delay by blocking the current thread.
///
/// # Purpose
/// This UDF is intended primarily for testing purposes, to simulate long-running queries or
/// evaluate the behavior of concurrency limits and timeout mechanisms within the query execution engine.
///
/// # SQL Usage
/// ```sql
/// SELECT sleep(5);
/// ```
///
/// This will block the executing thread for 5 seconds before returning the value `5`.
///
/// # Notes
/// - This UDF performs blocking (`thread::sleep`), and should **not** be used in production settings,
///   as it will block the execution thread and may reduce system throughput.
/// - The function is marked as `Volatile`, indicating that it has side effects (i.e., sleeping)
///   and cannot be optimized away by the query planner.
#[allow(clippy::unwrap_used)]
fn sleep_udf() -> ScalarUDF {
    let sleep_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let seconds = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(secs))) => *secs,
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected Int64Array".to_string()))?;
                array.value(0)
            }
            ColumnarValue::Scalar(_) => 0,
        };
        thread::sleep(Duration::from_secs(seconds.try_into().unwrap()));
        let result = Int64Array::from(vec![seconds]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    });
    create_udf(
        "sleep",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        sleep_fn,
    )
}
