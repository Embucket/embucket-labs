use axum::{Router, routing::get, routing::post, routing::delete};

use crate::state::AppState;
use crate::handlers::storage_profiles::{
    create_storage_profile,
    get_storage_profile,    
    delete_storage_profile,
    list_storage_profiles,
};
use crate::handlers::warehouses::{
    create_warehouse,
    get_warehouse,
    delete_warehouse,
    list_warehouses,
};


pub fn create_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { "Hello, World!" }))            
        .route("/v1/storage-profile", post(create_storage_profile))
        .route("/v1/storage-profile/:id", get(get_storage_profile))
        .route("/v1/storage-profile/:id", delete(delete_storage_profile))
        .route("/v1/storage-profile", get(list_storage_profiles))
        .route("/v1/warehouse", post(create_warehouse))
        .route("/v1/warehouse/:id", get(get_warehouse))
        .route("/v1/warehouse/:id", delete(delete_warehouse))
        .route("/v1/warehouse", get(list_warehouses))
        .with_state(state)
}


#[cfg(test)]
mod tests {
    use crate::handlers::storage_profiles;

    use super::*;
    use std::sync::Arc;
    use async_trait::async_trait;
    use uuid::Uuid;
    use control_plane::repository::InMemoryStorageProfileRepository;
    use control_plane::repository::InMemoryWarehouseRepository;
    use control_plane::service::ControlServiceImpl;
    use control_plane::service::ControlService;
    use control_plane::models::{StorageProfile, StorageProfileCreateRequest};
    use control_plane::models::{Warehouse, WarehouseCreateRequest};
    use control_plane::error::{Error, Result};
    use tower::{Service, ServiceExt};
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use http_body_util::BodyExt; // for `collect`
    use serde_json::{json, Value};


    fn create_router() -> Router {
        let storage_profile_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let storage_profile_service = Arc::new(ControlServiceImpl::new(storage_profile_repo, warehouse_repo));
        let app_state = AppState::new(storage_profile_service);
        create_app(app_state)
    }

    #[tokio::test]
    async fn test_create_storage_profile() {
        let app = create_router();
        let payload = json!({
            "type": "aws",
            "region": "us-west-2",
            "bucket": "my-bucket",
            "credentials": {
                "credential_type": "access_key",
                "aws_access_key_id": "my-access-key",
                "aws_secret_access_key": "my-secret-access-key"
            }
        });
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/storage-profile")
                    .method(http::Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_error_handling() {
        struct MockStorageProfileService;
        
        #[async_trait]
        impl ControlService for MockStorageProfileService {
            async fn create_profile(&self, _params: &StorageProfileCreateRequest) -> Result<StorageProfile> {
                Err(Error::InvalidInput("Invalid input".to_string()))
            }
            async fn get_profile(&self, _id: Uuid) -> Result<StorageProfile> {
                unimplemented!()
            }
            async fn delete_profile(&self, _id: Uuid) -> Result<()> {
                unimplemented!()
            }
            async fn list_profiles(&self) -> Result<Vec<StorageProfile>> {
                unimplemented!()
            }
            async fn create_warehouse(&self, _params: &WarehouseCreateRequest) -> Result<Warehouse> {
                unimplemented!()
            }
            async fn get_warehouse(&self, _id: Uuid) -> Result<Warehouse> {
                unimplemented!()
            }
            async fn delete_warehouse(&self, _id: Uuid) -> Result<()> {
                unimplemented!()
            }
            async fn list_warehouses(&self) -> Result<Vec<Warehouse>> {
                unimplemented!()
            }
        }
        let storage_profile_service = Arc::new(MockStorageProfileService{});
        let app_state = AppState::new(storage_profile_service);
        let app = create_app(app_state);
        
        // Mock service that returns an error
        let payload = json!({
            "type": "aws",
            "region": "us-west-2",
            "bucket": "my-bucket",
            "credentials": {
                "credential_type": "access_key",
                "aws_access_key_id": "wrong-access-key",
                "aws_secret_access_key": "wrong-secret-access"
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/storage-profile")
                    .method(http::Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}