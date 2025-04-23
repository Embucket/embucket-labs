use std::sync::Arc;
use datafusion::prelude::SessionContext;
use crate::execution::datafusion::functions::table::flatten::FlattenTableFunc;

pub mod flatten;

pub fn register_table_funcs(ctx: &SessionContext) {
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
    
}