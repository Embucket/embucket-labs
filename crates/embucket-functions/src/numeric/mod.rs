use std::sync::Arc;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use crate::numeric::div0::Div0Func;

pub mod div0;

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