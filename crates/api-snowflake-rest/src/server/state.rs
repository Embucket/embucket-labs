use super::server_models::Config;
use core_executor::ExecutionAppState;
use core_executor::service::ExecutionService;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub execution_svc: Arc<dyn ExecutionService>,
    pub config: Config,
}

impl ExecutionAppState for AppState {
    fn get_execution_svc(&self) -> Arc<dyn ExecutionService> {
        self.execution_svc.clone()
    }
}
