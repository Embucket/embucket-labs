use super::errors::ExpectedInt64ArraySnafu;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionImplementation, create_udf};
use snafu::OptionExt;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
#[must_use]
pub fn get_udf() -> Arc<ScalarUDF> {
    let sleep_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let seconds = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(secs))) => *secs,
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context(ExpectedInt64ArraySnafu)?;
                array.value(0)
            }
            ColumnarValue::Scalar(_) => 0,
        };
        thread::sleep(Duration::from_secs(seconds.try_into().unwrap()));
        let result = Int64Array::from(vec![seconds]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    });
    Arc::new(create_udf(
        "sleep",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        sleep_fn,
    ))
}
