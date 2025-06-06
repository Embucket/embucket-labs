pub(crate) mod cli;

use api_iceberg_rest::router::create_router as create_iceberg_router;
use api_iceberg_rest::state::Config as IcebergConfig;
use api_iceberg_rest::state::State as IcebergAppState;
use api_internal_rest::router::create_router as create_internal_router;
use api_internal_rest::state::State as InternalAppState;
use api_sessions::{RequestSessionMemory, RequestSessionStore};
use api_snowflake_rest::router::create_router as create_snowflake_router;
use api_snowflake_rest::schemas::Config;
use api_snowflake_rest::state::AppState as SnowflakeAppState;
use api_ui::auth::layer::require_auth;
use api_ui::auth::router::create_router as create_ui_auth_router;
use api_ui::config::AuthConfig as UIAuthConfig;
use api_ui::config::WebConfig as UIWebConfig;
use api_ui::layers::make_cors_middleware;
use api_ui::router::create_router as create_ui_router;
use api_ui::router::ui_open_api_spec;
use api_ui::state::AppState as UIAppState;
use api_ui::web_assets::config::StaticWebConfig;
use api_ui::web_assets::server::run_web_assets_server;
use axum::middleware;
use axum::{
    Json, Router,
    routing::{get, post},
};
use clap::Parser;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as ExecutionConfig;
use core_history::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use dotenv::dotenv;
use object_store::path::Path;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use slatedb::{Db as SlateDb, config::DbOptions};
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use time::Duration;
use tokio::signal;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, SessionManagerLayer};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::OpenApi;
use utoipa::openapi;
use utoipa_swagger_ui::SwaggerUi;

#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

const TARGETS: [&str; 10] = [
    "embucketd",
    "api_ui",
    "api_sessions",
    "api_snowflake_rest",
    "api_iceberg_rest",
    "core_executor",
    "core_utils",
    "core_history",
    "core_metastore",
    "df_catalog",
];

#[tokio::main]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::too_many_lines
)]
async fn main() {
    dotenv().ok();

    let opts = cli::CliOpts::parse();

    let provider = setup_tracing(&opts);

    let slatedb_prefix = opts.slatedb_prefix.clone();
    let data_format = opts
        .data_format
        .clone()
        .unwrap_or_else(|| "json".to_string());
    let snowflake_rest_cfg = Config::new(&data_format).expect("Failed to create snowflake config");
    let execution_cfg = ExecutionConfig::new().expect("Failed to create execution config");
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
    let static_web_config = StaticWebConfig {
        host: web_config.host.clone(),
        port: opts.assets_port.unwrap(),
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
    let history_store = Arc::new(SlateDBHistoryStore::new(db.clone()));

    let execution_svc = Arc::new(CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(execution_cfg),
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

    let internal_router =
        create_internal_router().with_state(InternalAppState::new(metastore.clone()));
    let ui_state = UIAppState::new(
        metastore.clone(),
        history_store,
        execution_svc.clone(),
        Arc::new(web_config.clone()),
        Arc::new(auth_config),
    );
    let ui_router = create_ui_router().with_state(ui_state.clone());
    let ui_router = ui_router.layer(middleware::from_fn_with_state(
        ui_state.clone(),
        require_auth,
    ));
    let ui_auth_router = create_ui_auth_router().with_state(ui_state.clone());
    let snowflake_router = create_snowflake_router().with_state(SnowflakeAppState {
        execution_svc,
        config: snowflake_rest_cfg,
    });
    let iceberg_router = create_iceberg_router().with_state(IcebergAppState {
        metastore,
        config: Arc::new(iceberg_config),
    });

    // --- OpenAPI specs ---
    let mut spec = ApiDoc::openapi();
    if let Some(extra_spec) = load_openapi_spec() {
        spec = spec.merge_from(extra_spec);
    }

    let ui_spec = ui_open_api_spec();

    let ui_router = Router::new()
        .nest("/ui", ui_router)
        .nest("/ui/auth", ui_auth_router);
    let ui_router = match web_config.allow_origin {
        Some(allow_origin) => ui_router.layer(make_cors_middleware(&allow_origin)),
        None => ui_router,
    };

    let router = Router::new()
        .merge(ui_router)
        .nest("/v1/metastore", internal_router)
        .merge(snowflake_router)
        .nest("/catalog", iceberg_router)
        .merge(
            SwaggerUi::new("/")
                .url("/openapi.json", spec)
                .url("/ui_openapi.json", ui_spec),
        )
        .route("/health", get(|| async { Json("OK") }))
        .route("/telemetry/send", post(|| async { Json("OK") }))
        .layer(session_layer)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(1200)))
        .layer(CatchPanicLayer::new())
        .into_make_service_with_connect_info::<SocketAddr>();

    // Runs static assets server in background
    run_web_assets_server(&static_web_config)
        .await
        .expect("Failed to start static assets server");

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

    provider
        .shutdown()
        .expect("TracerProvider should shutdown successfully");
}

#[allow(clippy::expect_used)]
fn setup_tracing(opts: &cli::CliOpts) -> SdkTracerProvider {
    // Initialize OTLP exporter using gRPC (Tonic)
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create OTLP exporter");

    let resource = Resource::builder().with_service_name("Em").build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let targets_with_level = |level: LevelFilter| -> Vec<(&str, LevelFilter)> {
        // let default_log_targets: Vec<(String, LevelFilter)> =
        TARGETS.iter().map(|t| ((*t), level)).collect()
    };

    tracing_subscriber::registry()
        // Telemetry filtering
        .with(
            tracing_opentelemetry::OpenTelemetryLayer::new(provider.tracer("embucket"))
                .with_level(true)
                .with_filter(
                    Targets::default()
                        .with_targets(targets_with_level(opts.tracing_level.clone().into())),
                ),
        )
        // Logs filtering
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::CLOSE)
                .with_filter(match std::env::var("RUST_LOG") {
                    Ok(val) => match val.parse::<Targets>() {
                        // env var parse OK
                        Ok(log_targets_from_env) => log_targets_from_env,
                        Err(err) => {
                            eprintln!("Failed to parse RUST_LOG: {err:?}");
                            Targets::default()
                                .with_targets(targets_with_level(LevelFilter::DEBUG))
                                .with_default(LevelFilter::DEBUG)
                        }
                    },
                    // No var set: use default log level INFO
                    _ => Targets::default()
                        .with_targets(targets_with_level(LevelFilter::INFO))
                        .with_default(LevelFilter::INFO),
                }),
        )
        .init();

    provider
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

// TODO: Fix OpenAPI spec generation
#[derive(OpenApi)]
#[openapi()]
pub struct ApiDoc;

fn load_openapi_spec() -> Option<openapi::OpenApi> {
    let openapi_yaml_content = fs::read_to_string("rest-catalog-open-api.yaml").ok()?;
    let mut original_spec = serde_yaml::from_str::<openapi::OpenApi>(&openapi_yaml_content).ok()?;
    // Dropping all paths from the original spec
    original_spec.paths = openapi::Paths::new();
    Some(original_spec)
}
