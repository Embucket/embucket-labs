use crate::schemas::Config;
use core_executor::service::ExecutionService;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub execution_svc: Arc<dyn ExecutionService>,
    pub config: Config,
}
