use crate::table::flatten::FlattenTableFunc;
use crate::table::result_scan::ResultScanFunc;
use core_history::HistoryStore;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub mod flatten;
pub mod result_scan;

pub fn register_udtfs(ctx: &SessionContext, history_store: Arc<dyn HistoryStore>) {
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
    ctx.register_udtf("result_scan", Arc::new(ResultScanFunc::new(history_store)));
}
