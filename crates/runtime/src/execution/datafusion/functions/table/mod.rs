use crate::execution::datafusion::functions::table::flatten::FlattenTableFunc;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub mod flatten;

pub fn register_table_funcs(ctx: &SessionContext) {
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
}
