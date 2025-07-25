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
pub use errors::Error;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        add_months::get_udf(),
        convert_timezone::get_udf(),
        date_add::get_udf(),
        date_diff::get_udf(),
        date_from_parts::get_udf(),
        dayname::get_udf(),
        last_day::get_udf(),
        monthname::get_udf(),
        next_day::get_udf(),
        previous_day::get_udf(),
        time_from_parts::get_udf(),
        timestamp_from_parts::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    // Register date part extract functions with default settings
    date_part_extract::register_date_part_extract_udfs_default(registry)?;

    Ok(())
}

/// Register datetime UDFs with custom week settings
pub fn register_udfs_with_week_settings(
    registry: &mut dyn FunctionRegistry,
    week_start: usize,
    week_of_year_policy: usize,
) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        add_months::get_udf(),
        convert_timezone::get_udf(),
        date_add::get_udf(),
        date_diff::get_udf(),
        date_from_parts::get_udf(),
        dayname::get_udf(),
        last_day::get_udf(),
        monthname::get_udf(),
        next_day::get_udf(),
        previous_day::get_udf(),
        time_from_parts::get_udf(),
        timestamp_from_parts::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    // Register date part extract functions with custom week settings
    date_part_extract::register_date_part_extract_udfs(registry, week_start, week_of_year_policy)?;

    Ok(())
}
