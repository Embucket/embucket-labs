pub mod cancel_query;
pub mod errors;
pub mod sleep;
pub mod typeof_func;

use datafusion_common::Result;
use datafusion_expr::registry::FunctionRegistry;
pub use errors::Error;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(typeof_func::get_udf())?;
    registry.register_udf(cancel_query::get_udf())?;
    // sleep is not for production use, returned udf created with `create_udf`
    registry.register_udf(sleep::get_udf())?;
    Ok(())
}
