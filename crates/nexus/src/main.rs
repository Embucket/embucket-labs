use axum::http::Method;
use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
use catalog::service::CatalogImpl;
use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
use control_plane::service::ControlServiceImpl;
use dotenv::dotenv;
use http_body_util::BodyExt;
use object_store_for_slatedb::{
    aws::AmazonS3Builder, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory,
    path::Path, ObjectStore,
};
use slatedb::config::DbOptions;
use slatedb::db::Db as SlateDb;
use std::env;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utils::Db;

pub mod http {
    pub mod router;
    pub mod control {
        pub mod handlers;
        pub mod router;
        pub mod schemas;
    }
    pub mod catalog {
        pub mod handlers;
        pub mod router;
        pub mod schemas;
    }

    pub mod ui {
        pub mod handlers;
        pub mod models;
        pub mod router;
    }
}
pub mod error;
pub mod state;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let object_store_backend =
        env::var("OBJECT_STORE_BACKEND").expect("OBJECT_STORE_BACKEND must be set");

    let object_store: Box<dyn ObjectStore> = match object_store_backend.as_str() {
        "s3" => {
            let aws_access_key_id =
                env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
            let aws_secret_access_key =
                env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
            let aws_region = env::var("AWS_REGION").expect("AWS_REGION must be set");
            let s3_bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");
            let s3_endpoint = env::var("S3_ENDPOINT").ok(); // Optional
            let s3_allow_http = env::var("S3_ALLOW_HTTP")
                .ok()
                .unwrap()
                .parse::<bool>()
                .expect("Failed to parse  S3_ALLOW_HTTP"); // Optional

            let s3_builder = AmazonS3Builder::new()
                .with_access_key_id(&aws_access_key_id)
                .with_secret_access_key(&aws_secret_access_key)
                .with_region(&aws_region)
                .with_bucket_name(&s3_bucket)
                .with_conditional_put(S3ConditionalPut::ETagMatch);

            let s3 = if let Some(endpoint) = s3_endpoint {
                s3_builder
                    .with_endpoint(&endpoint)
                    .with_allow_http(s3_allow_http)
                    .build()
                    .expect(
                        "Failed to create \
                S3 client",
                    )
            } else {
                s3_builder.build().expect("Failed to create S3 client")
            };

            Box::new(s3)
        }
        "file" => {
            let file_storage_path =
                env::var("FILE_STORAGE_PATH").expect("FILE_STORAGE_PATH must be set");
            let local_fs = LocalFileSystem::new_with_prefix(file_storage_path)
                .expect("Failed to create LocalFileSystem");
            Box::new(local_fs)
        }
        "memory" => {
            let memory = InMemory::new();
            Box::new(memory)
        }
        _ => {
            panic!("STORAGE_TYPE must be either 's3' or 'file' or 'memory'");
        }
    };

    let slatedb_prefix = env::var("SLATEDB_PREFIX").expect("SLATEDB_PREFIX must be set");

    let db = {
        let options = DbOptions::default();
        let db = Arc::new(Db::new(
            SlateDb::open_with_opts(Path::from(slatedb_prefix), options, object_store.into())
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

    // Create the application state
    let app_state = state::AppState::new(Arc::new(control_svc), Arc::new(catalog_svc));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app =
        http::router::create_app(app_state).layer(middleware::from_fn(print_request_response));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn print_request_response(
    req: Request,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (req_parts, req_body) = req.into_parts();
    let method = req_parts.method.to_string();
    let uri = req_parts.uri.to_string();
    let bytes = buffer_and_print("request", &method, &uri, req_body).await?;
    let req = Request::from_parts(req_parts, Body::from(bytes));

    let res = next.run(req).await;

    let (resp_parts, resp_body) = res.into_parts();
    let bytes = buffer_and_print("response", &method, &uri, resp_body).await?;
    let res = Response::from_parts(resp_parts, Body::from(bytes));

    Ok(res)
}

async fn buffer_and_print<B>(
    direction: &str,
    method: &String,
    uri: &String,
    body: B,
) -> Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data=Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read {direction} body: {err}"),
            ));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        tracing::debug!("{direction} {method} {uri} body = {body:?}");
    }

    Ok(bytes)
}
