use std::sync::Arc;
use control_plane::repository::InMemoryStorageProfileRepository;
use control_plane::repository::InMemoryWarehouseRepository;
use control_plane::service::{ControlService, ControlServiceImpl};

mod router;
mod state;
mod handlers;
mod schemas;
mod error;

#[tokio::main]
async fn main() {
        // Initialize the repository and concrete service implementation
        let storage_profile_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let storage_profile_service = Arc::new(ControlServiceImpl::new(storage_profile_repo, warehouse_repo));
    
        // Create the application state
        let app_state = state::AppState::new(storage_profile_service);
    
        // Create the application router and pass the state
        let app = router::create_app(app_state);
    
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, app).await.unwrap();
}