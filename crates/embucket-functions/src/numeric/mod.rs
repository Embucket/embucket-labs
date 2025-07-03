use crate::numeric::div0::Div0Func;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod div0;
pub mod errors;

pub use errors::Error;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        Arc::new(ScalarUDF::from(Div0Func::new(false))),
        Arc::new(ScalarUDF::from(Div0Func::new(true))),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
