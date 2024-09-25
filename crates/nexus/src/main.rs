use std::sync::Arc;
use control_plane::repository::InMemoryStorageProfileRepository;
use control_plane::service::{StorageProfileService, StorageProfileServiceImpl};

mod router;
mod state;
mod handlers;
mod schemas;

#[tokio::main]
async fn main() {
        // Initialize the repository and concrete service implementation
        let repository = Arc::new(InMemoryStorageProfileRepository::new());
        let storage_profile_service = Arc::new(StorageProfileServiceImpl::new(repository)) as Arc<dyn StorageProfileService + Send + Sync>;
    
        // Create the application state
        let app_state = state::AppState::new(storage_profile_service);
    
        // Create the application router and pass the state
        let app = router::create_app(app_state);
    
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, app).await.unwrap();
}