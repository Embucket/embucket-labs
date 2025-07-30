pub mod typeof_func;

use datafusion_expr::registry::FunctionRegistry;
use datafusion_common::Result;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(typeof_func::get_udf().into())?;
    Ok(())
}
