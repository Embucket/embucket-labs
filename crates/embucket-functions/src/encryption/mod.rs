use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod decrypt_raw;
pub mod encrypt_raw;
pub mod errors;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![encrypt_raw::get_udf(), decrypt_raw::get_udf()];
    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
