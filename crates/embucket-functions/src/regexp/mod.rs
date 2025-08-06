pub mod errors;
pub mod regexp_instr;

use crate::regexp::regexp_instr::RegexpInstrFunc;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
pub use errors::Error;
use std::sync::Arc;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![Arc::new(ScalarUDF::from(RegexpInstrFunc::new()))];
    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
