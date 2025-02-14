use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
use catalog::service::CatalogImpl;
use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
use control_plane::service::ControlServiceImpl;
use http_body_util::BodyExt;
use object_store::{path::Path, ObjectStore};
use slatedb::config::DbOptions;
use slatedb::db::Db as SlateDb;
use std::sync::Arc;
use time::Duration;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, SessionManagerLayer};

use http::layers::make_cors_middleware;
use http::session::{RequestSessionMemory, RequestSessionStore};
use utils::Db;

pub mod error;
pub mod http;
pub mod state;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_icebucket(
    state_store: Box<dyn ObjectStore>,
    slatedb_prefix: String,
    host: String,
    port: u16,
    allow_origin: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = {
        let options = DbOptions::default();
        Arc::new(Db::new(
            SlateDb::open_with_opts(Path::from(slatedb_prefix), options, state_store.into())
                .await?,
        ))
    };

    // Initialize the repository and concrete service implementation
    let control_svc = {
        let storage_profile_repo = StorageProfileRepositoryDb::new(db.clone());
        let warehouse_repo = WarehouseRepositoryDb::new(db.clone());
        ControlServiceImpl::new(Arc::new(storage_profile_repo), Arc::new(warehouse_repo))
    };
    let control_svc = Arc::new(control_svc);

    let session_memory = RequestSessionMemory::default();
    let session_store = RequestSessionStore::new(session_memory.clone(), control_svc.clone());

    tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );

    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(5 * 60)));

    let catalog_svc = {
        let t_repo = TableRepositoryDb::new(db.clone());
        let db_repo = DatabaseRepositoryDb::new(db.clone());

        CatalogImpl::new(Arc::new(t_repo), Arc::new(db_repo))
    };

    // Create the application state
    let app_state = state::AppState::new(control_svc.clone(), Arc::new(catalog_svc));

    let mut app = http::router::create_app(app_state)
        .layer(session_layer)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(print_request_response));

    if let Some(allow_origin) = allow_origin {
        app = app.layer(make_cors_middleware(allow_origin)?);
    }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(db))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

/// This func will wait for a signal to shutdown the service.
/// It will wait for either a Ctrl+C signal or a SIGTERM signal.
///
/// # Panics
/// If the function fails to install the signal handler, it will panic.
#[allow(clippy::expect_used, clippy::redundant_pub_crate)]
async fn shutdown_signal(_db: Arc<Db>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::warn!("Ctrl+C received, starting graceful shutdown");
            //db.close().await.unwrap();
        },
        () = terminate => {
            tracing::warn!("SIGTERM received, starting graceful shutdown");
            //db.close().await.unwrap();
        },
    }

    tracing::warn!("signal received, starting graceful shutdown");
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
    B: axum::body::HttpBody<Data = Bytes> + Send,
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
        // Skip upload endpoint logs as they can be large
        if !uri.contains("upload") {
            tracing::debug!("{direction} {method} {uri} body = {body}");
        }
    }

    Ok(bytes)
}
