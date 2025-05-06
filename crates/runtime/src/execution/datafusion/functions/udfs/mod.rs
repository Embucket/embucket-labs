use datafusion::{common::Result, prelude::SessionContext};

mod convert_timezone;
mod date_add;
mod date_diff;
mod date_from_parts;
pub mod variant;
//pub mod geospatial;
pub mod parse_json;

mod time_from_parts;
mod timestamp_from_parts;
use variant::register_udfs as register_variant_udfs;

pub fn register_udfs(ctx: &mut SessionContext) -> Result<()> {
    convert_timezone::register_udf(ctx);
    date_add::register_udf(ctx);
    parse_json::register_udf(ctx);
    date_diff::register_udf(ctx);
    timestamp_from_parts::register_udf(ctx);
    time_from_parts::register_udf(ctx);
    date_from_parts::register_udf(ctx);

    register_variant_udfs(ctx)?;

    Ok(())
}

mod macros {
    macro_rules! make_udf_function {
        ($udf_type:ty) => {
            paste::paste! {
                pub fn register_udf(ctx: &mut datafusion::prelude::SessionContext) {
                    ctx.register_udf(datafusion_expr::ScalarUDF::from(<$udf_type>::default()));
                }
            }
        };
    }
    pub(crate) use make_udf_function;
}
