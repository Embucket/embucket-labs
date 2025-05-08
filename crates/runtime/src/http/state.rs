use embucket_history::history_store::WorksheetsStore;
use embucket_metastore::metastore::Metastore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::AuthConfig;
use crate::execution::service::ExecutionService;
use crate::http::config::WebConfig;

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub metastore: Arc<dyn Metastore + Send + Sync>,
    pub history_store: Arc<dyn WorksheetsStore + Send + Sync>,
    pub execution_svc: Arc<dyn ExecutionService>,
    pub dbt_sessions: Arc<Mutex<HashMap<String, String>>>,
    pub config: Arc<WebConfig>,
    // separate non printable AuthConfig
    pub auth_config: Arc<AuthConfig>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(
        metastore: Arc<dyn Metastore + Send + Sync>,
        history_store: Arc<dyn WorksheetsStore + Send + Sync>,
        execution_svc: Arc<dyn ExecutionService>,
        config: Arc<WebConfig>,
        auth_config: Arc<AuthConfig>,
    ) -> Self {
        Self {
            metastore,
            history_store,
            execution_svc,
            dbt_sessions: Arc::new(Mutex::default()),
            config,
            auth_config,
        }
    }
}
