use crate::table::flatten::FlattenTableFunc;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub mod flatten;

pub fn register_udtfs(ctx: &SessionContext) {
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
}
