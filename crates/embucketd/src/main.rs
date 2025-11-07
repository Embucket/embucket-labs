// Set this clippy directive to suppress clippy::needless_for_each warnings
// until following issue will be fixed https://github.com/juhaku/utoipa/issues/1420
#![allow(clippy::needless_for_each)]
pub(crate) mod cli;
pub(crate) mod helpers;
pub(crate) mod layers;
pub(crate) mod metastore_config;

use api_snowflake_rest::server::layer::require_auth as snowflake_require_auth;
use api_snowflake_rest::server::router::create_auth_router as create_snowflake_auth_router;
use api_snowflake_rest::server::router::create_router as create_snowflake_router;
use api_snowflake_rest::server::server_models::Config;
use api_snowflake_rest::server::state::AppState as SnowflakeAppState;
use axum::middleware;
use axum::{
    Json, Router,
    routing::{get, post},
};
use clap::Parser;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as ExecutionConfig;
use core_metastore::InMemoryMetastore;
use dotenv::dotenv;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::runtime::TokioCurrentThread;
use opentelemetry_sdk::trace::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor as BatchSpanProcessorAsyncRuntime;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::filter::{FilterExt, LevelFilter, Targets, filter_fn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::metastore_config::MetastoreBootstrapConfig;
// use core_sqlite::SqliteDb;

#[cfg(feature = "alloc-tracing")]
mod alloc_tracing {
    pub use crate::layers::AllocLogLayer;
    pub use tracing_allocations::{TRACE_ALLOCATOR, TracingAllocator};

    #[global_allocator]
    static ALLOCATOR: TracingAllocator<tikv_jemallocator::Jemalloc> =
        TracingAllocator::new(tikv_jemallocator::Jemalloc);
}

#[cfg(not(feature = "alloc-tracing"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const TARGETS: [&str; 10] = [
    "embucketd",
    "api_snowflake_rest",
    "core_executor",
    "core_metastore",
    "df_catalog",
    "datafusion",
    "iceberg_rust",
    "datafusion_iceberg",
    "tower",
    "tower_http",
];

#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::too_many_lines
)]
fn main() {
    dotenv().ok();

    let opts = cli::CliOpts::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start({
            move || {
                #[cfg(feature = "alloc-tracing")]
                if opts.alloc_tracing.unwrap_or(false) {
                    alloc_tracing::TRACE_ALLOCATOR.with(|cell| *cell.borrow_mut() = true);
                }
            }
        })
        .build()
        .expect("build tokio runtime");

    rt.block_on(async move {
        let tracing_provider = setup_tracing(&opts);

        let _ = async_main(opts, tracing_provider).await;
    });
}

#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
async fn async_main(
    opts: cli::CliOpts,
    tracing_provider: SdkTracerProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_format = opts
        .data_format
        .clone()
        .unwrap_or_else(|| "json".to_string());
    let snowflake_rest_cfg = Config::new(&data_format)
        .expect("Failed to create snowflake config")
        .with_demo_credentials(
            opts.auth_demo_user.clone().unwrap(),
            opts.auth_demo_password.clone().unwrap(),
        );

    // Bootstrap the service if no flag is present (`--no-bootstrap`) with:
    // 1. Creation of a default in-memory volume named `embucket`
    // 2. Creation of a default database `embucket` in the volume `embucket`
    // 3. Creation of a default schema `public` in the database `embucket`

    let execution_cfg = ExecutionConfig {
        embucket_version: "0.1.0".to_string(),
        bootstrap_default_entities: !opts.no_bootstrap,
        sql_parser_dialect: opts.sql_parser_dialect.clone(),
        query_timeout_secs: opts.query_timeout_secs,
        max_concurrency_level: opts.max_concurrency_level,
        mem_pool_type: opts.mem_pool_type,
        mem_pool_size_mb: opts.mem_pool_size_mb,
        mem_enable_track_consumers_pool: opts.mem_enable_track_consumers_pool,
        disk_pool_size_mb: opts.disk_pool_size_mb,
        query_history_rows_limit: opts.query_history_rows_limit,
    };
    let host = opts.host.clone().unwrap();
    let port = opts.port.unwrap();
    let metastore = Arc::new(InMemoryMetastore::new());

    if let Some(config_path) = &opts.metastore_config {
        tracing::info!(
            path = %config_path.display(),
            "Bootstrapping metastore from config"
        );
        let config = MetastoreBootstrapConfig::load(config_path.as_path()).await?;
        config.apply(metastore.clone()).await?;
    }

    tracing::info!("Creating execution service");
    let execution_svc = Arc::new(
        CoreExecutionService::new(metastore.clone(), Arc::new(execution_cfg))
            .await
            .expect("Failed to create execution service"),
    );
    tracing::info!("Execution service created");

    let snowflake_state = SnowflakeAppState {
        execution_svc: execution_svc.clone(),
        config: snowflake_rest_cfg,
    };
    let compression_layer = ServiceBuilder::new()
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new());
    let snowflake_router = create_snowflake_router()
        .with_state(snowflake_state.clone())
        .layer(compression_layer.clone())
        .layer(middleware::from_fn_with_state(
            snowflake_state.clone(),
            snowflake_require_auth,
        ));
    let snowflake_auth_router = create_snowflake_auth_router()
        .with_state(snowflake_state.clone())
        .layer(compression_layer);
    let snowflake_router = snowflake_router.merge(snowflake_auth_router);

    // --- OpenAPI specs ---
    let swagger = SwaggerUi::new("/").url("/openapi.json", ApiDoc::openapi());

    let router = Router::new()
        .merge(snowflake_router)
        .merge(swagger)
        .route("/health", get(|| async { Json("OK") }))
        .route("/telemetry/send", post(|| async { Json("OK") }))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(1200)))
        .layer(CatchPanicLayer::new())
        .into_make_service_with_connect_info::<SocketAddr>();

    let web_addr = helpers::resolve_ipv4(format!("{host}:{port}"))
        .expect("Failed to resolve web server address");
    let listener = tokio::net::TcpListener::bind(web_addr)
        .await
        .expect("Failed to bind to address");
    let addr = listener.local_addr().expect("Failed to get local address");
    tracing::info!(%addr, "Listening on http");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Failed to start server");

    tracing_provider
        .shutdown()
        .expect("TracerProvider should shutdown successfully");

    Ok(())
}

#[allow(clippy::expect_used, clippy::redundant_closure_for_method_calls)]
fn setup_tracing(opts: &cli::CliOpts) -> SdkTracerProvider {
    // Initialize OTLP exporter using gRPC (Tonic)
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create OTLP exporter");

    let resource = Resource::builder().with_service_name("Em").build();

    // Since BatchSpanProcessor and BatchSpanProcessorAsyncRuntime are not compatible with each other
    // we just create TracerProvider with different span processors
    let tracing_provider = match opts.tracing_span_processor {
        cli::TracingSpanProcessor::BatchSpanProcessor => SdkTracerProvider::builder()
            .with_span_processor(BatchSpanProcessor::builder(exporter).build())
            .with_resource(resource)
            .build(),
        cli::TracingSpanProcessor::BatchSpanProcessorExperimentalAsyncRuntime => {
            SdkTracerProvider::builder()
                .with_span_processor(
                    BatchSpanProcessorAsyncRuntime::builder(exporter, TokioCurrentThread).build(),
                )
                .with_resource(resource)
                .build()
        }
    };

    let targets_with_level =
        |targets: &[&'static str], level: LevelFilter| -> Vec<(&str, LevelFilter)> {
            // let default_log_targets: Vec<(String, LevelFilter)> =
            targets.iter().map(|t| ((*t), level)).collect()
        };

    // Memory allocations
    #[cfg(feature = "alloc-tracing")]
    let alloc_layer =
        alloc_tracing::AllocLogLayer::write_to_file("./alloc.log").expect("open alloc log");

    #[cfg(feature = "alloc-tracing")]
    {
        let alloc_flusher = Arc::new(alloc_layer.clone());
        alloc_flusher.spawn_flusher(std::time::Duration::from_secs(1));
    }

    let registry = tracing_subscriber::registry()
        // Telemetry filtering
        .with(
            tracing_opentelemetry::OpenTelemetryLayer::new(tracing_provider.tracer("embucket"))
                .with_level(true)
                .with_filter(Targets::default().with_targets(targets_with_level(
                    &TARGETS,
                    opts.tracing_level.clone().into(),
                ))),
        )
        // Logs filtering
        .with({
            let fmt_filter = match std::env::var("RUST_LOG") {
                Ok(val) => match val.parse::<Targets>() {
                    Ok(log_targets_from_env) => log_targets_from_env,
                    Err(err) => {
                        eprintln!("Failed to parse RUST_LOG: {err:?}");
                        Targets::default()
                            .with_targets(targets_with_level(&TARGETS, LevelFilter::DEBUG))
                            .with_default(LevelFilter::DEBUG)
                    }
                },
                _ => Targets::default()
                    .with_targets(targets_with_level(&TARGETS, LevelFilter::INFO))
                    .with_targets(targets_with_level(
                        &["tower_sessions", "tower_sessions_core", "tower_http"],
                        LevelFilter::OFF,
                    ))
                    .with_default(LevelFilter::INFO),
            };
            // Skip memory allocations spans
            let spans_always = filter_fn(|meta| meta.is_span());
            let not_alloc_event = filter_fn(|meta| {
                meta.target() != "alloc" && meta.target() != "tracing_allocations"
            });

            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_span_events(FmtSpan::NONE)
                .json()
                .with_filter(spans_always.or(not_alloc_event.and(fmt_filter)))
        });

    // Memory allocations layer
    #[cfg(feature = "alloc-tracing")]
    let registry = registry.with(alloc_layer.with_filter(filter_fn(|meta| {
        meta.target() == "tracing_allocations" || meta.target() == "alloc"
    })));
    registry.init();
    tracing_provider
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
async fn shutdown_signal() {
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
        },
        () = terminate => {
            tracing::warn!("SIGTERM received, starting graceful shutdown");
        },
    }

    tracing::warn!("signal received, starting graceful shutdown");
}

// TODO: Fix OpenAPI spec generation
#[derive(OpenApi)]
#[openapi()]
pub struct ApiDoc;
