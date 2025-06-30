pub mod booland;
pub mod boolor;
pub mod boolxor;
pub mod equal_null;
pub mod iff;
pub mod nullifzero;
pub mod zeroifnull;

use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;
// Re-export the get_udf functions
pub use booland::get_udf as booland_get_udf;
pub use boolor::get_udf as boolor_get_udf;
pub use boolxor::get_udf as boolxor_get_udf;
pub use equal_null::get_udf as equal_null_get_udf;
pub use iff::get_udf as iff_get_udf;
pub use nullifzero::get_udf as nullifzero_get_udf;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        booland::get_udf(),
        boolor::get_udf(),
        boolxor::get_udf(),
        equal_null::get_udf(),
        iff::get_udf(),
        nullifzero::get_udf(),
        zeroifnull::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
