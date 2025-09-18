pub mod errors;
pub mod to_boolean;
pub mod to_time;
pub mod to_varchar;

pub mod to_array;
pub mod to_binary;
pub mod to_date;
pub mod to_decimal;
mod to_object;
pub mod to_timestamp;
pub mod to_variant;

use crate::conversion::to_date::ToDateFunc;
use crate::conversion::to_decimal::ToDecimalFunc;
use crate::conversion::to_timestamp::ToTimestampFunc;
use crate::session_params::SessionParams;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::array::{ArrayRef, TimestampMicrosecondBuilder, TimestampNanosecondBuilder};
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
pub use errors::Error;
use std::sync::Arc;
pub use to_binary::ToBinaryFunc;
pub use to_boolean::ToBooleanFunc;
pub use to_time::ToTimeFunc;
pub use to_varchar::ToVarcharFunc;

pub fn register_udfs(
    registry: &mut dyn FunctionRegistry,
    session_params: &Arc<SessionParams>,
) -> datafusion_common::Result<()> {
    let mut functions: Vec<Arc<ScalarUDF>> = vec![
        to_array::get_udf(),
        to_variant::get_udf(),
        Arc::new(ScalarUDF::from(ToBinaryFunc::new(false))),
        Arc::new(ScalarUDF::from(ToBinaryFunc::new(true))),
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(false))),
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(true))),
        Arc::new(ScalarUDF::from(ToTimeFunc::new(false))),
        Arc::new(ScalarUDF::from(ToTimeFunc::new(true))),
        Arc::new(ScalarUDF::from(ToDecimalFunc::new(false))),
        Arc::new(ScalarUDF::from(ToDecimalFunc::new(true))),
        Arc::new(ScalarUDF::from(ToDateFunc::new(false))),
        Arc::new(ScalarUDF::from(ToDateFunc::new(true))),
        Arc::new(ScalarUDF::from(ToVarcharFunc::new(false))),
        Arc::new(ScalarUDF::from(ToVarcharFunc::new(true))),
        to_object::get_udf(),
    ];

    // Add timestamp functions
    let timestamp_functions = [
        (false, "to_timestamp".to_string()),
        (true, "try_to_timestamp".to_string()),
        (false, "to_timestamp_ntz".to_string()),
        (true, "try_to_timestamp_ntz".to_string()),
        (false, "to_timestamp_tz".to_string()),
        (true, "try_to_timestamp_tz".to_string()),
        (false, "to_timestamp_ltz".to_string()),
        (true, "try_to_timestamp_ltz".to_string()),
    ];
    for (r#try, name) in timestamp_functions {
        functions.push(Arc::from(ScalarUDF::from(ToTimestampFunc::new(
            r#try,
            name,
            session_params.clone(),
        ))));
    }

    for func in functions {
        registry.register_udf(func)?;
    }
    Ok(())
}

trait TimestampBuilder {
    type Builder;

    fn new(cap: usize, tz: Option<Arc<str>>) -> Self::Builder;
    fn append_value(b: &mut Self::Builder, v: i64);
    fn append_null(b: &mut Self::Builder);
    fn finish(b: Self::Builder) -> ArrayRef;
    fn unit() -> TimeUnit;
}

struct Nano;
struct Micro;

impl TimestampBuilder for Nano {
    type Builder = TimestampNanosecondBuilder;
    fn new(cap: usize, tz: Option<Arc<str>>) -> Self::Builder {
        TimestampNanosecondBuilder::with_capacity(cap).with_timezone_opt(tz)
    }
    fn append_value(b: &mut Self::Builder, v: i64) {
        b.append_value(v);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> ArrayRef {
        Arc::new(b.finish())
    }

    fn unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

impl TimestampBuilder for Micro {
    type Builder = TimestampMicrosecondBuilder;
    fn new(cap: usize, tz: Option<Arc<str>>) -> Self::Builder {
        TimestampMicrosecondBuilder::with_capacity(cap).with_timezone_opt(tz)
    }
    fn append_value(b: &mut Self::Builder, v: i64) {
        b.append_value(v);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> ArrayRef {
        Arc::new(b.finish())
    }
    fn unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}
