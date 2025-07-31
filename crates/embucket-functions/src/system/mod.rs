pub mod typeof_func;

use datafusion_common::Result;
use datafusion_expr::registry::FunctionRegistry;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(typeof_func::get_udf())?;
    Ok(())
}
