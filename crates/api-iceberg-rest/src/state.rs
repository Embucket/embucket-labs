use core_metastore::metastore::Metastore;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub iceberg_catalog_url: String,
}

#[derive(Clone)]
pub struct State {
    pub metastore: Arc<dyn Metastore + Send + Sync>,
    pub config: Arc<Config>,
    pub bearer_token: Option<String>,
}

impl State {
    // You can add helper methods for state initialization if needed
    pub fn new(
        metastore: Arc<dyn Metastore + Send + Sync>,
        config: Arc<Config>,
        bearer_token: Option<String>,
    ) -> Self {
        Self {
            metastore,
            config,
            bearer_token,
        }
    }
}
