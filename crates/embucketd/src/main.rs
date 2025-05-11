pub(crate) mod cli;

use api_iceberg_rest::router::create_router as create_iceberg_router;
use api_iceberg_rest::state::Config as IcebergConfig;
use api_iceberg_rest::state::State as IcebergAppState;
use api_internal_rest::router::create_router as create_internal_router;
use api_internal_rest::state::State as InternalAppState;
use api_sessions::{RequestSessionMemory, RequestSessionStore};
use api_snowflake_rest::router::create_router as create_snowflake_router;
use api_snowflake_rest::state::AppState as SnowflakeAppState;
use api_ui::config::AuthConfig as UIAuthConfig;
use api_ui::config::WebConfig as UIWebConfig;
use api_ui::router::create_router as create_ui_router;
use api_ui::state::AppState as UIAppState;
use axum::{
    Json, Router,
    routing::{get, post},
};
use clap::Parser;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as ExecutionConfig;
use core_history::RecordingExecutionService;
use core_history::SlateDBWorksheetsStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use dotenv::dotenv;
use object_store::path::Path;
use slatedb::{Db as SlateDb, config::DbOptions};
use std::sync::Arc;
use time::Duration;
use tokio::signal;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, SessionManagerLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]
async fn main() {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "embucketd=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let opts = cli::CliOpts::parse();
    let slatedb_prefix = opts.slatedb_prefix.clone();
    let data_format = opts
        .data_format
        .clone()
        .unwrap_or_else(|| "json".to_string());
    let execution_cfg =
        ExecutionConfig::new(&data_format).expect("Failed to create execution config");
    let mut auth_config = UIAuthConfig::new(opts.jwt_secret());
    auth_config.with_demo_credentials(
        opts.auth_demo_user.clone().unwrap(),
        opts.auth_demo_password.clone().unwrap(),
    );
    let web_config = UIWebConfig {
        host: opts.host.clone().unwrap(),
        port: opts.port.unwrap(),
        allow_origin: opts.cors_allow_origin.clone(),
    };
    let iceberg_config = IcebergConfig {
        iceberg_catalog_url: opts.catalog_url.clone().unwrap(),
    };

    let object_store = opts
        .object_store_backend()
        .expect("Failed to create object store");
    let db = Db::new(Arc::new(
        SlateDb::open_with_opts(
            Path::from(slatedb_prefix),
            DbOptions::default(),
            object_store.clone(),
        )
        .await
        .expect("Failed to start Slate DB"),
    ));

    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBWorksheetsStore::new(db.clone()));

    let execution_svc = Arc::new(CoreExecutionService::new(metastore.clone(), execution_cfg));
    let execution_svc = Arc::new(RecordingExecutionService::new(
        execution_svc,
        history_store.clone(),
    ));

    let session_memory = RequestSessionMemory::default();
    let session_store = RequestSessionStore::new(session_memory, execution_svc.clone());

    tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );

    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(5 * 60)));

    let _internal_router: Router<InternalAppState> =
        create_internal_router().with_state(InternalAppState::new(metastore.clone()));
    let ui_router = create_ui_router().with_state(UIAppState::new(
        metastore.clone(),
        history_store,
        execution_svc.clone(),
        Arc::new(web_config.clone()),
        Arc::new(auth_config),
    ));
    let snowflake_router =
        create_snowflake_router().with_state(SnowflakeAppState { execution_svc });
    let iceberg_router = create_iceberg_router().with_state(IcebergAppState {
        metastore,
        config: Arc::new(iceberg_config),
    });

    // TODO: Restore internal router + restore database handlers
    // TODO: Restore auth router
    // TODO: Restore CORS settings
    // TODO: Restore web assets serving
    // TOOD: Restore OpenAPI spec serving
    // TODO: Reconsider .expect() calls back to errors
    // TODO: Restore build script to build web assets
    let router = Router::new()
        // .nest("/v1/metastore", internal_router)
        .nest("/ui", ui_router)
        .nest("/v1", snowflake_router)
        .nest("/catalog", iceberg_router)
        .route("/health", get(|| async { Json("OK") }))
        .route("/telemetry/send", post(|| async { Json("OK") }))
        .layer(session_layer)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(1200)))
        .layer(CatchPanicLayer::new());

    let host = web_config.host.clone();
    let port = web_config.port;
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
        .await
        .expect("Failed to bind to address");
    let addr = listener.local_addr().expect("Failed to get local address");
    tracing::info!("Listening on http://{}", addr);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(Arc::new(db.clone())))
        .await
        .expect("Failed to start server");
}

/// This func will wait for a signal to shutdown the service.
/// It will wait for either a Ctrl+C signal or a SIGTERM signal.
///
/// # Panics
/// If the function fails to install the signal handler, it will panic.
#[allow(
    clippy::expect_used,
    clippy::redundant_pub_crate,
    clippy::cognitive_complexity
)]
async fn shutdown_signal(db: Arc<Db>) {
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
            db.close().await.expect("Failed to close database");
            tracing::warn!("Ctrl+C received, starting graceful shutdown");
        },
        () = terminate => {
            db.close().await.expect("Failed to close database");
            tracing::warn!("SIGTERM received, starting graceful shutdown");
        },
    }

    tracing::warn!("signal received, starting graceful shutdown");
}
