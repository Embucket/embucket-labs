use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod add_months;
pub mod convert_timezone;
pub mod date_add;
pub mod date_diff;
pub mod date_from_parts;
pub mod date_part_extract;
pub mod dayname;
pub mod errors;
pub mod last_day;
pub mod monthname;
pub mod next_day;
pub mod previous_day;
pub mod time_from_parts;
pub mod timestamp_from_parts;
use crate::datetime::convert_timezone::ConvertTimezoneFunc;
use crate::datetime::date_diff::DateDiffFunc;
use crate::datetime::last_day::LastDayFunc;
use crate::session_params::SessionParams;
pub use errors::Error;

pub fn register_udfs(
    registry: &mut dyn FunctionRegistry,
    session_params: &Arc<SessionParams>,
) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        add_months::get_udf(),
        date_add::get_udf(),
        Arc::new(ScalarUDF::from(DateDiffFunc::new(session_params.clone()))),
        date_from_parts::get_udf(),
        dayname::get_udf(),
        Arc::new(ScalarUDF::from(LastDayFunc::new(session_params.clone()))),
        Arc::new(ScalarUDF::from(ConvertTimezoneFunc::new(
            session_params.clone(),
        ))),
        monthname::get_udf(),
        next_day::get_udf(),
        previous_day::get_udf(),
        time_from_parts::get_udf(),
        timestamp_from_parts::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }
    date_part_extract::register_udfs(registry, session_params)?;
    Ok(())
}
