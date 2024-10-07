use catalog::repository::InMemoryCatalogRepository;
use control_plane::repository::InMemoryStorageProfileRepository;
use control_plane::repository::InMemoryWarehouseRepository;
use control_plane::service::{ControlService, ControlServiceImpl};
use std::sync::Arc;

pub mod http {
    pub mod router;
    pub mod control {
        pub mod handlers;
        pub mod schemas;
    }
}
pub mod error;
pub mod state;

#[tokio::main]
async fn main() {
    // Initialize the repository and concrete service implementation
    let storage_profile_service = {
        let storage_profile_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        ControlServiceImpl::new(storage_profile_repo, warehouse_repo)
    };

    let catalog_repo = {
        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        let warehouse_location = "/tmp/warehouse";
        InMemoryCatalogRepository::new(file_io, Some(warehouse_location.to_string()))
    };

    // Create the application state
    let app_state = state::AppState::new(Arc::new(storage_profile_service), Arc::new(catalog_repo));

    // Create the application router and pass the state
    let app = http::router::create_app(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
