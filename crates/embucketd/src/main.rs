pub(crate) mod cli;
pub(crate) mod error; // Added error module

use crate::error::{
    ApplicationSnafu, AsyncTaskSnafu, CliError, CliErrorKind, CliResult, ConfigLoadIoSnafu,
    ConfigLoadSnafu, InitializationFailedSnafu, IoSnafu, MissingEnvVarSnafu, TracingSetupSnafu, UtilsSnafu, MetastoreSnafu, ExecutorSnafu, ConfigParsingSnafu, // Added ConfigParsingSnafu
};
use api_iceberg_rest::router::create_router as create_iceberg_router;
use api_iceberg_rest::state::Config as IcebergConfig;
use api_iceberg_rest::state::State as IcebergAppState;
use api_internal_rest::router::create_router as create_internal_router;
use api_internal_rest::state::State as InternalAppState;
use api_sessions::{RequestSessionMemory, RequestSessionStore};
use api_snowflake_rest::router::create_router as create_snowflake_router;
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
    Router,
    routing::{get, post}, Json,
};
use clap::Parser;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as ExecutionConfig; // Renamed for clarity
use core_history::RecordingExecutionService;
use core_history::SlateDBWorksheetsStore;
use core_metastore::SlateDBMetastore;
use core_utils::{Db, Error as CoreUtilsError};
use dotenv::dotenv;
use embucket_errors::wrap_error;
use object_store::path::Path as ObjectStorePath;
use slatedb::{config::DbOptions, Db as SlateDb};
use snafu::prelude::*;
use std::fs;
use std::process::ExitCode;
use std::sync::Arc;
use time::Duration;
use tokio::signal;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, SessionManagerLayer};
use tracing; // Ensure tracing is imported
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utoipa::OpenApi;
use utoipa::openapi;
use utoipa_swagger_ui::SwaggerUi;

#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn main() -> ExitCode {
    // Initialize tracing early if run_app is not called, or if run_app fails before tracing init.
    // However, run_app initializes tracing, so this is mainly for run_app failing super early.
    // For robust tracing of early errors, initialization would need to be the very first thing.
    // Given run_app initializes tracing, we'll primarily rely on that.

    if let Err(e) = run_app() {
        // Check if tracing was initialized. If not, eprintln is the fallback.
        // This is a best-effort: if tracing init failed, logs might go to stderr.
        // If tracing init succeeded, this will use the configured tracing subscribers.
        
        let error_message = e.to_string();
        let mut backtrace_string = "No backtrace available.".to_string();

        match &e {
            CliError::Metastore { source, .. } |
            CliError::Executor { source, .. } |
            CliError::Io { source, .. } |
            CliError::ConfigParsing { source, .. } |
            CliError::Utils { source, .. } |
            CliError::AsyncTask { source, .. } |
            CliError::Application { source, .. } => {
                if let Some(bt) = source.get_backtrace_opt() { // Assuming get_backtrace_opt returns Option<&Backtrace>
                    backtrace_string = bt.to_string();
                }
            }
        }

        if std::env::var("RUST_BACKTRACE").map_or(false, |val| val == "1" || val == "full") ||
           std::env::var("EMBUCKET_DEBUG").map_or(false, |val| val == "1" || val.eq_ignore_ascii_case("true")) {
            tracing::error!(
                error.message = %error_message,
                error.backtrace = %backtrace_string,
                "Application failed to start or encountered a fatal error."
            );
        } else {
            tracing::error!(
                error.message = %error_message,
                "Application failed to start or encountered a fatal error. Set RUST_BACKTRACE=1 or EMBUCKET_DEBUG=1 for full backtrace."
            );
        }
        // Also print to stderr for visibility if tracing is not configured to output to console.
        eprintln!("Error: {}", error_message);
        if std::env::var("RUST_BACKTRACE").map_or(false, |val| val == "1" || val == "full") ||
           std::env::var("EMBUCKET_DEBUG").map_or(false, |val| val == "1" || val.eq_ignore_ascii_case("true")) {
            if backtrace_string != "No backtrace available." {
                 eprintln!("\nBacktrace:\n{}", backtrace_string);
            }
        }

        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[tokio::main]
#[allow(
    clippy::too_many_lines
)]
async fn run_app() -> CliResult<()> {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "embucketd=info,tower_http=info,api_iceberg_rest=info".into()), // Adjusted default level
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .map_err(|e| wrap_error(CliErrorKind::TracingSetup { message: e.to_string() }, "Failed to initialize tracing".to_string()))
        .context(ApplicationSnafu)?;

    tracing::info!("Tracing initialized.");

    let opts = cli::CliOpts::parse();
    tracing::debug!(cli_options = ?opts, "Parsed CLI options");

    let slatedb_prefix = opts.slatedb_prefix.clone();
    let data_format = opts.data_format.clone().unwrap_or_else(|| "json".to_string());

    tracing::info!(data_format = %data_format, "Using data format for execution config.");
    let execution_config = ExecutionConfig::new(&data_format) // Renamed variable
        .map_err(|e| {
            // Assuming ExecutionConfig::new returns an error that is EmbucketErrorSource compatible
            // For example, if it's a simple struct error that impl Display+Debug+Send+Sync+'static
            // Or if it's already an EmbucketError or a type that can be wrapped.
            // If e is not EmbucketErrorSource compatible, it needs to be converted first.
            // For now, assuming 'e' is compatible or ExecutionError can wrap it.
            let exec_err = core_executor::error::ExecutionError::DataFusion { // Example: mapping to a known ExecutionError variant
                source: wrap_error( // This assumes DataFusionError can be an EmbucketErrorSource
                    datafusion_common::DataFusionError::External { source: Box::new(e), backtrace: snafu::Backtrace::capture() }, // Example construction
                    "ExecutionConfig creation failed".to_string()
                )
            };
            wrap_error(exec_err, "Failed to create execution configuration".to_string())
        })
        .context(ExecutorSnafu)?;


    let jwt_secret = opts.jwt_secret().map_err(|e| {
        wrap_error(CliErrorKind::MissingEnvVar { name: "JWT_SECRET".to_string(), reason: e.to_string()},
        "JWT secret configuration error".to_string())
    }).context(ApplicationSnafu)?;
    tracing::debug!("JWT secret configured.");
    let mut auth_config = UIAuthConfig::new(jwt_secret);

    let demo_user = opts.auth_demo_user.clone().unwrap_or_default();
    let demo_pass = opts.auth_demo_password.clone().unwrap_or_default();
    if !demo_user.is_empty() {
        tracing::info!("Demo user credentials configured.");
        auth_config.with_demo_credentials(demo_user, demo_pass);
    }

    let web_config = UIWebConfig {
        host: opts.host.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
        port: opts.port.unwrap_or(8080),
        allow_origin: opts.cors_allow_origin.clone(),
    };
    tracing::info!(web_host = %web_config.host, web_port = %web_config.port, "Web server configuration loaded.");

    let iceberg_config = IcebergConfig {
        iceberg_catalog_url: opts.catalog_url.clone().unwrap_or_default(),
    };
    tracing::info!(catalog_url = %iceberg_config.iceberg_catalog_url, "Iceberg catalog URL configured.");

    let static_web_config = StaticWebConfig {
        host: web_config.host.clone(),
        port: opts.assets_port.unwrap_or(8081),
    };
    tracing::info!(assets_host = %static_web_config.host, assets_port = %static_web_config.port, "Static assets server configuration loaded.");


    let object_store = opts.object_store_backend()
        .map_err(|e| {
            let core_utils_err = CoreUtilsError::Configuration { message: e.to_string() };
            wrap_error(core_utils_err, "Failed to create object store backend".to_string())
        })
        .context(UtilsSnafu)?;
    tracing::info!("Object store backend configured.");

    let db_path = ObjectStorePath::from(slatedb_prefix.clone());
    tracing::info!(slatedb_path = %db_path, "Initializing SlateDB.");
    let db_instance = SlateDb::open_with_opts(
        db_path.clone(),
        DbOptions::default(),
        object_store.clone(),
    )
    .await
    .map_err(|e| { // e is slatedb::SlateDBError
        // We need to wrap this appropriately. Let's assume MetastoreError has a variant for SlateDBError.
        // core_metastore::error::MetastoreErrorKind::SlateDB { source: e } - this is not how it works.
        // MetastoreError::SlateDB { source: EmbucketError<slatedb::SlateDBError> }
        // So, first wrap e in EmbucketError, then that in MetastoreError.
        let embucket_slatedb_err = wrap_error(e, format!("Failed to open SlateDB at {}", db_path));
        let metastore_err = MetastoreError::SlateDB { source: embucket_slatedb_err }; // This should be MetastoreSnafu
        // Then wrap metastore_err in EmbucketError for CliError
        wrap_error(metastore_err, "SlateDB initialization failed".to_string())
    })
    .context(MetastoreSnafu)?; // Changed to MetastoreSnafu, assuming it can wrap a MetastoreError directly.
                               // This implies MetastoreError itself is EmbucketErrorSource, which is true.
    let db = Db::new(Arc::new(db_instance));
    tracing::info!("SlateDB initialized successfully.");

    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    tracing::debug!("Metastore service created.");
    let history_store = Arc::new(SlateDBWorksheetsStore::new(db.clone()));
    tracing::debug!("History store service created.");

    let core_exec_svc = CoreExecutionService::new(metastore.clone(), execution_config)
        .map_err(|e| wrap_error(e, "Failed to create CoreExecutionService".to_string()))
        .context(ExecutorSnafu)?;
    let execution_svc_base = Arc::new(core_exec_svc); // Renamed for clarity
    tracing::debug!("Core execution service created.");

    let execution_svc_hist_recorder = Arc::new(RecordingExecutionService::new( // Renamed for clarity
        execution_svc_base.clone(),
        history_store.clone(),
    ));
    tracing::debug!("Recording execution service created.");

    let session_memory = RequestSessionMemory::default();
    let session_store = RequestSessionStore::new(session_memory, execution_svc_hist_recorder.clone());
    tracing::debug!("Session store configured.");

    tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );
    tracing::info!("Session expiration task spawned.");

    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(5 * 60)));
    tracing::debug!("Session layer configured.");

    let internal_router =
        create_internal_router().with_state(InternalAppState::new(metastore.clone()));
    let ui_state = UIAppState::new(
        metastore.clone(),
        history_store,
        execution_svc_hist_recorder.clone(),
        Arc::new(web_config.clone()),
        Arc::new(auth_config),
    );
    let ui_router_auth_required = create_ui_router().with_state(ui_state.clone());
    let ui_router_auth_required = ui_router_auth_required.layer(middleware::from_fn_with_state(
        ui_state.clone(),
        require_auth,
    ));
    let ui_auth_router = create_ui_auth_router().with_state(ui_state.clone());

    let snowflake_router =
        create_snowflake_router().with_state(SnowflakeAppState { execution_svc: execution_svc_base.clone() }); // Base for Snowflake
    let iceberg_router = create_iceberg_router().with_state(IcebergAppState {
        metastore,
        config: Arc::new(iceberg_config),
    });
    tracing::debug!("API routers created.");

    let mut spec = ApiDoc::openapi();
    match load_openapi_spec().await {
        Ok(Some(extra_spec)) => {
            spec = spec.merge_from(extra_spec);
            tracing::info!("Optional OpenAPI spec loaded and merged.");
        }
        Ok(None) => { /* Optional spec not found, already logged in load_openapi_spec */ }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to load optional OpenAPI spec. Proceeding without it.");
            // Convert 'e' (CliError) to a warning or handle as per requirements.
            // For now, just log and continue. If it were critical, bubble up `Err(e)`.
        }
    }


    let ui_spec = ui_open_api_spec();

    let ui_router_combined = Router::new()
        .nest("/ui", ui_router_auth_required)
        .nest("/ui/auth", ui_auth_router);
    let ui_router_final = match web_config.allow_origin {
        Some(allow_origin) => {
            tracing::info!(cors_origin = %allow_origin, "CORS middleware enabled for UI router.");
            ui_router_combined.layer(make_cors_middleware(&allow_origin))
        }
        None => ui_router_combined,
    };

    let router = Router::new()
        .merge(ui_router_final)
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
        .layer(CatchPanicLayer::new());
    tracing::debug!("Main application router configured.");

    run_web_assets_server(&static_web_config)
        .await
        .map_err(|e_str| { // e_str is String from run_web_assets_server
            let kind = CliErrorKind::InitializationFailed { service_name: "StaticAssetsServer".to_string() };
            // wrap_error takes E: EmbucketErrorSource. String is not.
            // So, the 'e_str' should be part of 'kind' or the context string for 'wrap_error'.
            // Let's make it part of the CliErrorKind.
            let kind_with_detail = CliErrorKind::InitializationFailed { service_name: format!("StaticAssetsServer: {}", e_str) };
            wrap_error(kind_with_detail, "Static assets server failed to start".to_string())
        })
        .context(ApplicationSnafu)?;
    tracing::info!(host = %static_web_config.host, port = %static_web_config.port, "Static assets server started.");


    let host = web_config.host.clone();
    let port = web_config.port;
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
        .await
        .map_err(|e| wrap_error(e, format!("Failed to bind HTTP server to {}:{}", host, port)))
        .context(IoSnafu)?;

    let addr = listener.local_addr()
        .map_err(|e| wrap_error(e, "Failed to get local address of HTTP server".to_string()))
        .context(IoSnafu)?;

    tracing::info!("Main application server listening on http://{}", addr);

    let db_for_shutdown = db.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(db_for_shutdown))
        .await
        .map_err(|e| wrap_error(e, "Axum server failed".to_string())) // e is std::io::Error
        .context(IoSnafu)?;

    tracing::info!("Application shutdown complete.");
    Ok(())
}


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
            tracing::warn!("Ctrl+C received, initiating graceful shutdown...");
        },
        () = terminate => {
            tracing::warn!("SIGTERM received, initiating graceful shutdown...");
        },
    }
    
    tracing::info!("Attempting to close database connection...");
    if let Err(e) = db.close().await { // e is CoreUtilsError
        // Log this error. Since we're not in a CliResult context, direct logging is best.
        let embucket_db_close_err = wrap_error(e, "Failed to close database gracefully during shutdown".to_string());
        // Constructing a CliError just for logging display if needed, or log parts directly.
        let cli_err_for_log = UtilsSnafu { source: embucket_db_close_err }.build();
        tracing::error!(error = %cli_err_for_log, "Database graceful shutdown failed.");
        // Also to stderr for immediate visibility during shutdown
        eprintln!("Error during shutdown, failed to close database gracefully: {}", cli_err_for_log);
    } else {
        tracing::info!("Database closed successfully during shutdown.");
    }
    tracing::warn!("Graceful shutdown process completed. Server will now exit.");
}

#[derive(OpenApi)]
#[openapi()]
pub struct ApiDoc;

async fn load_openapi_spec() -> CliResult<Option<openapi::OpenApi>> {
    let path = "rest-catalog-open-api.yaml";
    tracing::debug!(openapi_spec_path = %path, "Attempting to load optional OpenAPI spec.");
    match fs::read_to_string(path) {
        Ok(openapi_yaml_content) => {
            let mut original_spec: openapi::OpenApi = serde_yaml::from_str(&openapi_yaml_content)
                .map_err(|e| wrap_error(e, format!("Failed to parse OpenAPI spec from '{}'", path)))
                .context(ConfigParsingSnafu)?; // Ensure ConfigParsingSnafu is in error.rs for CliError
            original_spec.paths = openapi::Paths::new();
            tracing::info!(openapi_spec_path = %path, "Successfully loaded and processed optional OpenAPI spec.");
            Ok(Some(original_spec))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(openapi_spec_path = %path, "Optional OpenAPI spec file not found. Skipping.");
            Ok(None)
        }
        Err(e) => { // Other IO errors
            tracing::error!(openapi_spec_path = %path, error = %e, "Failed to read optional OpenAPI spec file.");
            Err(wrap_error(e, format!("Failed to read OpenAPI spec file '{}'", path)))
                .context(IoSnafu)
        }
    }
}
