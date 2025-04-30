use std::sync::Arc;

use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};

mod convert_timezone;
mod date_add;
mod date_diff;
mod date_from_parts;
//pub mod geospatial;
mod parse_json;
mod time_from_parts;
mod timestamp_from_parts;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        convert_timezone::get_udf(),
        date_add::get_udf(),
        parse_json::get_udf(),
        date_diff::get_udf(),
        timestamp_from_parts::get_udf(),
        time_from_parts::get_udf(),
        date_from_parts::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}

mod macros {
    macro_rules! make_udf_function {
    ($udf_type:ty) => {
        paste::paste! {
            static [< STATIC_ $udf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            pub fn get_udf() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
                [< STATIC_ $udf_type:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            <$udf_type>::default(),
                        ))
                    })
                    .clone()
            }
        }
    }
}

    pub(crate) use make_udf_function;
}
