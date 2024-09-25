use std::sync::Arc;
use control_plane::service::StorageProfileService; // Your service implementation

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub storage_profile_svc: Arc<dyn StorageProfileService + Send + Sync>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(service: Arc<dyn StorageProfileService + Send + Sync>) -> Self {
        Self {
            storage_profile_svc: service,
        }
    }
}