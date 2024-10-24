use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
use catalog::service::CatalogImpl;
use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
use control_plane::service::ControlServiceImpl;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use slatedb::config::DbOptions;
use slatedb::db::Db as SlateDb;
use std::sync::Arc;
use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use axum::http::Method;
use http_body_util::BodyExt;
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
        pub mod router;
        pub mod models;
    }
}
pub mod error;
pub mod state;

#[tokio::main]
async fn main() {
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

    let app = http::router::create_app(app_state)
        .layer(middleware::from_fn(print_request_response));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();



    // // Set up tracing to log to stdout
    // let subscriber = FmtSubscriber::builder()
    //     .with_max_level(Level::INFO)
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    //
    // // Create the application router and pass the state
    // let app = http::router::create_app(app_state).layer(middleware::from_fn(log_request));
    //
    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    // axum::serve(listener, app).await.unwrap();
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

async fn buffer_and_print<B>(direction: &str, method:&String, uri:&String, body: B) -> Result<Bytes, (StatusCode,
                                                                                                     String)>
where
    B: axum::body::HttpBody<Data = Bytes>,
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
