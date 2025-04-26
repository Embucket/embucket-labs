use axum::Router;
use nexus::repository::InMemoryStorageProfileRepository;
use nexus::router::create_app;
use nexus::service::StorageProfileServiceImpl;
use nexus::state::AppState;

fn create_app() -> Router {
    let repository = Arc::new(InMemoryStorageProfileRepository::new());
    let storage_profile_service = Arc::new(StorageProfileServiceImpl::new(repository));
    let app_state = AppState::new(storage_profile_service);
    create_app(app_state)
}

#[tokio::test]
async fn test_create_storage_profile() {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/storage-profile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
