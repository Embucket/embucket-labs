use core_metastore::metastore::Metastore;
use std::sync::Arc;

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct State {
    pub metastore: Arc<dyn Metastore + Send + Sync>,
}

impl State {
    // You can add helper methods for state initialization if needed
    pub fn new(metastore: Arc<dyn Metastore + Send + Sync>) -> Self {
        Self { metastore }
    }
}
