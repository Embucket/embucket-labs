use axum::extract::Path;
use axum::{routing::delete, routing::get, routing::post, Router};
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_scalar::{Scalar, Servable as ScalarServable};

use crate::handlers::get_config;
use crate::handlers::namespaces::{
    create_namespace, delete_namespace, get_namespace, list_namespaces,
};
use crate::handlers::storage_profiles::{
    create_storage_profile, delete_storage_profile, get_storage_profile, list_storage_profiles,
    StorageProfileApi,
};
use crate::handlers::tables::create_table;
use crate::handlers::warehouses::{
    create_warehouse, delete_warehouse, get_warehouse, list_warehouses,
};
use crate::state::AppState;

#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/v1/storage-profile", api = StorageProfileApi)
    ),
    tags(
        (name = "storage-profile", description = "Storage profile API")
    )
)]
struct ApiDoc;

pub fn create_app(state: AppState) -> Router {
    let sp_router = Router::new()
        .route("", post(create_storage_profile))
        .route(":id", get(get_storage_profile))
        .route(":id", delete(delete_storage_profile))
        .route("", get(list_storage_profiles));

    Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .nest("v1/storage-profile", sp_router)
        .route("/v1/warehouse", post(create_warehouse))
        .route("/v1/warehouse/:id", get(get_warehouse))
        .route("/v1/warehouse/:id", delete(delete_warehouse))
        .route("/v1/warehouse", get(list_warehouses))
        .route("/catalog/v1/:id/config", get(get_config))
        .route("/catalog/v1/:id/namespace", get(list_namespaces))
        .route("/catalog/v1/:id/namespace", post(create_namespace))
        .route(
            "/catalog/v1/:id/namespace/:namespace_id",
            get(get_namespace),
        )
        .route(
            "/catalog/v1/:id/namespace/:namespace_id",
            delete(delete_namespace),
        )
        .route(
            "/catalog/v1/:id/namespace/:namespace_id/table",
            post(create_table),
        )
        .merge(Scalar::with_url("/openapi", ApiDoc::openapi()))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::too_many_lines)]

    use crate::handlers::storage_profiles;
    use crate::schemas::namespaces::NamespaceSchema;
    use crate::schemas::storage_profiles::StorageProfile as StorageProfileSchema;
    use crate::schemas::warehouses::Warehouse as WarehouseSchema;

    use super::*;
    use async_trait::async_trait;
    use axum::http::request;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use catalog::repository::{InMemoryCatalogRepository, Repository};
    use control_plane::error::{Error, Result};
    use control_plane::models::{StorageProfile, StorageProfileCreateRequest};
    use control_plane::models::{Warehouse, WarehouseCreateRequest};
    use control_plane::repository::InMemoryStorageProfileRepository;
    use control_plane::repository::InMemoryWarehouseRepository;
    use control_plane::service::ControlService;
    use control_plane::service::ControlServiceImpl;
    use http_body_util::BodyExt; // for `collect`
    use iceberg::io::FileIOBuilder;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::{Service, ServiceExt};
    use uuid::Uuid;

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    fn new_memory_catalog() -> impl Repository {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let warehouse_location = temp_path();
        InMemoryCatalogRepository::new(file_io, Some(warehouse_location))
    }

    fn create_router() -> Router {
        let storage_profile_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let storage_profile_service = Arc::new(ControlServiceImpl::new(
            storage_profile_repo,
            warehouse_repo,
        ));
        let catalog_repo = Arc::new(new_memory_catalog());
        let app_state = AppState::new(storage_profile_service, catalog_repo);
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
    async fn test_namespace_get() {
        let mut app = create_router().into_service();
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
        let request = Request::builder()
            .uri("/v1/storage-profile")
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let sid = serde_json::from_slice::<StorageProfileSchema>(&body)
            .unwrap()
            .id;

        // Now create warehouse
        let payload = json!({
            "name": "my-warehouse",
            "storage_profile_id": sid,
            "prefix": "my-prefix",
        });
        let request = Request::builder()
            .uri("/v1/warehouse")
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let wid = serde_json::from_slice::<WarehouseSchema>(&body).unwrap().id;

        // Now create namespace
        let payload = json!({
            "namespace": ["my-namespace"],
            "properties": {
                "key": "value"
            }
        });
        let request = Request::builder()
            .uri(format!("/catalog/v1/{wid}/namespace"))
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let namespace_id = serde_json::from_slice::<NamespaceSchema>(&body)
            .unwrap()
            .namespace;
        let namespace_id = namespace_id.inner().first().unwrap().clone();

        // Now get namespace
        let request = Request::builder()
            .uri(format!("/catalog/v1/{wid}/namespace/{namespace_id}"))
            .method(http::Method::GET)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let namespace_id = serde_json::from_slice::<NamespaceSchema>(&body)
            .unwrap()
            .namespace;

        assert_eq!(namespace_id.inner(), vec!("my-namespace"));

        // Now let's create table
        let payload = json!({
            "name": "my-table",
            "location": "s3://my-bucket/my-prefix",
            "schema": {
                "fields": [
                    {
                        "name": "id",
                        "data_type": "int",
                        "nullable": false,
                        "default_value": null
                    },
                    {
                        "name": "name",
                        "data_type": "string",
                        "nullable": true,
                        "default_value": null
                    }
                ]
            },
            "partition_spec": {
                "columns": ["id"],
                "spec_id": "my-spec"
            },
            "write_order": "append",
            "stage_create": true,
            "properties": {
                "key": "value"
            }
        });
        let request = Request::builder()
            .uri(format!("/catalog/v1/{wid}/namespace/my-namespace/table"))
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_error_handling() {
        struct MockStorageProfileService;

        #[async_trait]
        impl ControlService for MockStorageProfileService {
            async fn create_profile(
                &self,
                _params: &StorageProfileCreateRequest,
            ) -> Result<StorageProfile> {
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
            async fn create_warehouse(
                &self,
                _params: &WarehouseCreateRequest,
            ) -> Result<Warehouse> {
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
        let storage_profile_service = Arc::new(MockStorageProfileService {});
        let catalog_repo = Arc::new(new_memory_catalog());
        let app_state = AppState::new(storage_profile_service, catalog_repo);
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
