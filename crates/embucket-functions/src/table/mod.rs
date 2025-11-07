use crate::table::flatten::func::FlattenTableFunc;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub mod errors;
pub mod flatten;
pub use errors::Error;

pub fn register_udtfs(ctx: &SessionContext) {
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
}
