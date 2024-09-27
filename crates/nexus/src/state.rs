use std::sync::Arc;
use control_plane::service::ControlService; // Your service implementation

// Define a State struct that contains shared services or repositories
#[derive(Clone)]
pub struct AppState {
    pub control_svc: Arc<dyn ControlService + Send + Sync>,
}

impl AppState {
    // You can add helper methods for state initialization if needed
    pub fn new(service: Arc<dyn ControlService + Send + Sync>) -> Self {
        Self {
            control_svc: service,
        }
    }
}