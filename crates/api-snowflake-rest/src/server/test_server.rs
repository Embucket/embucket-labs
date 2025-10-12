use super::server_models::Config;
use crate::server::router::make_app;
use core_executor::utils::Config as UtilsConfig;
use core_history::store::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::FmtSubscriber;

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

    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .finish();
    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("Failed to set global default subscriber: {err}");
    }

    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBHistoryStore::new(db));

    let app = make_app(metastore, history, app_cfg, execution_cfg)
        .await
        .unwrap()
        .into_make_service_with_connect_info::<SocketAddr>();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}
