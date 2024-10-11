use catalog::service::Catalog;
use control_plane::service::ControlService; // Your service implementation
use std::sync::Arc; // Your service implementation

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub control_svc: Arc<dyn ControlService + Send + Sync>,
    pub catalog_svc: Arc<dyn Catalog + Send + Sync>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(
        control_svc: Arc<dyn ControlService + Send + Sync>,
        catalog_repo: Arc<dyn Catalog + Send + Sync>,
    ) -> Self {
        Self {
            control_svc,
            catalog_svc: catalog_repo,
        }
    }
}
