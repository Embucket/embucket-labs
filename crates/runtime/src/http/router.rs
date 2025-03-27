// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use axum::routing::{get, post};
use axum::{Json, Router};
use std::fs;
use tower_http::catch_panic::CatchPanicLayer;
use utoipa::openapi::{self};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::http::catalog::router::create_router as create_iceberg_router;
use crate::http::dbt::router::create_router as create_dbt_router;
// use crate::http::ui::old_handlers::tables::ApiDoc as TableApiDoc;
use crate::http::state::AppState;
use crate::http::ui::router::{create_router as create_ui_router, ui_open_api_spec};
use tower_http::timeout::TimeoutLayer;

use super::metastore::router::create_router as create_metastore_router;

// TODO: Fix OpenAPI spec generation
#[derive(OpenApi)]
#[openapi()]
pub struct ApiDoc;

pub fn create_app(state: AppState) -> Router {
    let mut spec = ApiDoc::openapi();
    if let Some(extra_spec) = load_openapi_spec() {
        spec = spec.merge_from(extra_spec);
    }

    let ui_spec = ui_open_api_spec();
    // if let Some(extra_spec) = load_openapi_spec() {
    //     ui_spec = ui_spec.merge_from(extra_spec);
    // }
    let metastore_router = create_metastore_router();
    let ui_router = create_ui_router();
    let dbt_router = create_dbt_router();
    let iceberg_catalog = create_iceberg_router();

    Router::new()
        .merge(dbt_router)
        .merge(metastore_router)
        .nest("/ui", ui_router)
        .nest("/catalog", iceberg_catalog)
        .merge(
            SwaggerUi::new("/")
                .url("/openapi.json", spec)
                .url("/ui_openapi.json", ui_spec),
        )
        .route("/health", get(|| async { Json("OK") }))
        .route("/telemetry/send", post(|| async { Json("OK") }))
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(1200)))
        .layer(CatchPanicLayer::new())
        //.layer(super::layers::make_cors_middleware(allow_origin.unwrap_or("*".to_string())))
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
    #![allow(clippy::too_many_lines, clippy::unwrap_used)]

    /*use super::*;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
    };
    use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
    use catalog::service::CatalogImpl;
    use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
    use control_plane::service::ControlServiceImpl;
    use control_plane::utils::Config;
    use http_body_util::BodyExt;
    // for `collect`
    use object_store::{memory::InMemory, path::Path, ObjectStore};
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
            Arc::new(Db::new(
                Arc::new(SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                    .await
                    .unwrap(),
            )))
        };

        // Initialize the repository and concrete service implementation
        let control_svc = {
            let storage_profile_repo = StorageProfileRepositoryDb::new(db.clone());
            let warehouse_repo = WarehouseRepositoryDb::new(db.clone());
            let config = Config::new("json");
            ControlServiceImpl::new(
                Arc::new(storage_profile_repo),
                Arc::new(warehouse_repo),
                config,
            )
        };

        let catalog_svc = {
            let t_repo = TableRepositoryDb::new(db.clone());
            let db_repo = DatabaseRepositoryDb::new(db);

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
    async fn test_create_get_table() {
        let mut app = create_router().await.into_service();
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
            .uri(format!("/catalog/v1/{wid}/namespaces"))
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
            .uri(format!("/catalog/v1/{wid}/namespaces/{namespace_id}"))
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

        assert_eq!(namespace_id.inner(), vec!["my-namespace"]);

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
            .uri(format!("/catalog/v1/{wid}/namespace/my-namespace/table"))
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let _response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        //println!("{:?}", response.into_body().collect().await.unwrap());
        // assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[should_panic(expected = "not implemented")]
    async fn test_error_handling() {
        panic!("not implemented");

        /*let app = create_router().await;

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

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);*/
    }*/
}
