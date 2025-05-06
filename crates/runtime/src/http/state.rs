use embucket_history::{history_store::WorksheetsStore, auth_store::AuthStore};
use embucket_metastore::metastore::Metastore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::execution::service::ExecutionService;
use crate::http::config::WebConfig;
use crate::config::AuthConfig;

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub metastore: Arc<dyn Metastore + Send + Sync>,
    pub history: Arc<dyn WorksheetsStore + Send + Sync>,
    pub auth_store: Arc<dyn AuthStore + Send + Sync>,
    pub execution_svc: Arc<ExecutionService>,
    pub dbt_sessions: Arc<Mutex<HashMap<String, String>>>,
    pub config: Arc<WebConfig>,
    // separate non printable AuthConfig 
    pub auth_config: Arc<AuthConfig>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(
        metastore: Arc<dyn Metastore + Send + Sync>,
        history: Arc<dyn WorksheetsStore + Send + Sync>,
        auth_store: Arc<dyn AuthStore + Send + Sync>,
        execution_svc: Arc<ExecutionService>,
        config: Arc<WebConfig>,
        auth_config: Arc<AuthConfig>,
    ) -> Self {
        Self {
            metastore,
            history,
            auth_store,
            execution_svc,
            dbt_sessions: Arc::new(Mutex::default()),
            config,
            auth_config,
        }
    }
}

