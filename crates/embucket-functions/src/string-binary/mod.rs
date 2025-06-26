use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod insert;
pub mod jarowinkler_similarity;
pub mod length;
pub mod lower;
pub mod rtrimmed_length;
pub mod split;
pub mod strtok;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        insert::get_udf(),
        jarowinkler_similarity::get_udf(),
        length::get_udf(),
        lower::get_udf(),
        rtrimmed_length::get_udf(),
        split::get_udf(),
        strtok::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
