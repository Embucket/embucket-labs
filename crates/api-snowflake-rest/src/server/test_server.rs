use super::server_models::Config;
use crate::server::router::make_app;
use core_executor::utils::Config as UtilsConfig;
use core_history::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use std::net::SocketAddr;
use tracing_subscriber::fmt::format::FmtSpan;

#[allow(clippy::expect_used)]
pub async fn run_test_rest_api_server(data_format: &str) -> SocketAddr {
    let app_cfg = Config::new(data_format)
        .expect("Failed to create server config")
        .with_demo_credentials("embucket".to_string(), "embucket".to_string());

    run_test_rest_api_server_with_config(app_cfg, UtilsConfig::default()).await
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn run_test_rest_api_server_with_config(
    app_cfg: Config,
    execution_cfg: UtilsConfig,
) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let traces_writer = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("traces.log")
        .expect("Failed to open traces.log");

    let subscriber = tracing_subscriber::fmt()
        // using stderr as it won't be showed until test failed
        .with_writer(traces_writer)
        .with_ansi(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::NONE)
        .with_level(true)
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .finish();

    // ignoring error: as with parralel tests execution, just first thread is able to set it successfully
    // since all tests run in a single process
    let _ = tracing::subscriber::set_global_default(subscriber);

    let metastore = SlateDBMetastore::new_in_memory().await;
    let history = SlateDBHistoryStore::new_in_memory().await;

    let app = make_app(metastore, history, app_cfg, execution_cfg)
        .await
        .unwrap()
        .into_make_service_with_connect_info::<SocketAddr>();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}
