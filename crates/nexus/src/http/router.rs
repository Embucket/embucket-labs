use axum::routing::get;
use axum::{Json, Router};
use std::fs;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{Any, CorsLayer};
use utoipa::openapi::{self};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::http::catalog::router::create_router as create_catalog_router;
use crate::http::control::handlers::storage_profiles::StorageProfileApi;
use crate::http::control::handlers::warehouses::WarehouseApi;
use crate::http::control::router::create_router as create_control_router;
use crate::http::ui::handlers::databases::ApiDoc as DatabaseApiDoc;
use crate::http::ui::handlers::profiles::ApiDoc as ProfileApiDoc;
use crate::http::ui::handlers::tables::ApiDoc as TableApiDoc;
use crate::http::ui::handlers::warehouses::ApiDoc as WarehouseApiDoc;
use crate::http::ui::router::{create_router as create_ui_router, ApiDoc as UiApiDoc};
use crate::state::AppState;
use tower_http::timeout::TimeoutLayer;

#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/v1/storage-profile", api = StorageProfileApi, tags = ["storage-profiles"]),
        (path = "/v1/warehouse", api = WarehouseApi, tags = ["warehouses"]),
    ),
    tags(
        (name = "storage-profile", description = "Storage profile API"),
        (name = "warehouse", description = "Warehouse API"),
        (name = "ui", description = "Web UI API"),
    )
)]
pub struct ApiDoc;

pub fn create_app(state: AppState) -> Router {
    let mut spec = ApiDoc::openapi();
    if let Some(extra_spec) = load_openapi_spec() {
        spec = spec.merge_from(extra_spec);
    }

    let mut ui_spec = UiApiDoc::openapi()
        .merge_from(ProfileApiDoc::openapi())
        .merge_from(WarehouseApiDoc::openapi())
        .merge_from(TableApiDoc::openapi())
        .merge_from(DatabaseApiDoc::openapi());
    if let Some(extra_spec) = load_openapi_spec() {
        ui_spec = ui_spec.merge_from(extra_spec);
    }
    let catalog_router = create_catalog_router();
    let control_router = create_control_router();
    let ui_router = create_ui_router();

    Router::new()
        .nest("/", control_router)
        .nest("/catalog", catalog_router)
        .nest("/ui", ui_router)
        .merge(
            SwaggerUi::new("/")
                .url("/openapi.json", spec)
                .url("/ui_openapi.json", ui_spec),
        )
        .route("/health", get(|| async { Json("OK") }))
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)))
        .layer(CatchPanicLayer::new())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

fn load_openapi_spec() -> Option<openapi::OpenApi> {
    let openapi_yaml_content = fs::read_to_string("rest-catalog-open-api.yaml").ok()?;
    let mut original_spec = serde_yaml::from_str::<openapi::OpenApi>(&openapi_yaml_content).ok()?;
    // Dropping all paths from the original spec
    original_spec.paths = openapi::Paths::new();
    Some(original_spec)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::too_many_lines)]

    use crate::http::catalog::schemas::Namespace as NamespaceSchema;
    use crate::http::control::schemas::storage_profiles::StorageProfile as StorageProfileSchema;
    use crate::http::control::schemas::warehouses::Warehouse as WarehouseSchema;

    use super::*;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
    use catalog::service::CatalogImpl;
    use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
    use control_plane::service::ControlServiceImpl;
    use http_body_util::BodyExt;
    // for `collect`
    use object_store_for_slatedb::{memory::InMemory, path::Path, ObjectStore};
    use serde_json::json;
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tower::{Service, ServiceExt};
    use utils::Db;

    lazy_static::lazy_static! {
        static ref TEMP_DIR: TempDir = TempDir::new().unwrap();
    }

    async fn create_router() -> Router {
        let db = {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let options = DbOptions::default();
            let db = Arc::new(Db::new(
                SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                    .await
                    .unwrap(),
            ));
            db
        };

        // Initialize the repository and concrete service implementation
        let control_svc = {
            let storage_profile_repo = StorageProfileRepositoryDb::new(db.clone());
            let warehouse_repo = WarehouseRepositoryDb::new(db.clone());
            ControlServiceImpl::new(Arc::new(storage_profile_repo), Arc::new(warehouse_repo))
        };

        let catalog_svc = {
            let t_repo = TableRepositoryDb::new(db.clone());
            let db_repo = DatabaseRepositoryDb::new(db.clone());

            CatalogImpl::new(Arc::new(t_repo), Arc::new(db_repo))
        };

        let app_state = AppState::new(Arc::new(control_svc), Arc::new(catalog_svc));
        create_app(app_state)
    }

    #[tokio::test]
    async fn test_create_storage_profile() {
        let app = create_router().await;
        let payload = json!({
            "type": "aws",
            "region": "us-west-2",
            "bucket": "my-bucket",
            "credentials": {
                "credential-type": "access-key",
                "aws-access-key-id": "my-access-key",
                "aws-secret-access-key": "my-secret-access-key"
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
    async fn test_create_get_table() {
        let mut app = create_router().await.into_service();
        let payload = json!({
            "type": "aws",
            "region": "us-west-2",
            "bucket": "my-bucket",
            "credentials": {
                "credential-type": "access-key",
                "aws-access-key-id": "my-access-key",
                "aws-secret-access-key": "my-secret-access-key"
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
            "storage-profile-id": sid,
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
            .uri(format!("/catalog/{wid}/v1/namespace"))
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
            .uri(format!("/catalog/{wid}/v1/namespace/{namespace_id}"))
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
        "type": "struct",
        "schema": {
            "schema-id": 1,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "type": "int",
                    "required": true
                },
                {
                    "id": 2,
                    "name": "name",
                    "type": "string",
                    "required": true
                }
            ],
            "identifier-field-ids": [1]
        },
        });
        let request = Request::builder()
            .uri(format!("/catalog/{wid}/v1/namespace/my-namespace/table"))
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

        println!("{:?}", response.into_body().collect().await.unwrap());
        // assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[should_panic(expected = "not implemented")]
    async fn test_error_handling() {
        panic!("not implemented");

        let app = create_router().await;

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
