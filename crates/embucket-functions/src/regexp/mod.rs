pub mod errors;
pub mod regexp_instr;
pub mod regexp_like;
pub mod regexp_replace;
pub mod regexp_substr;
pub mod regexp_substr_all;

use crate::regexp::regexp_instr::RegexpInstrFunc;
use crate::regexp::regexp_like::RegexpLikeFunc;
use crate::regexp::regexp_replace::RegexpReplaceFunc;
use crate::regexp::regexp_substr::RegexpSubstrFunc;
use crate::regexp::regexp_substr_all::RegexpSubstrAllFunc;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
pub use errors::Error;
use std::sync::Arc;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        Arc::new(ScalarUDF::from(RegexpInstrFunc::new())),
        Arc::new(ScalarUDF::from(RegexpSubstrFunc::new())),
        Arc::new(ScalarUDF::from(RegexpReplaceFunc::new())),
        Arc::new(ScalarUDF::from(RegexpSubstrAllFunc::new())),
        Arc::new(ScalarUDF::from(RegexpLikeFunc::new())),
    ];
    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
