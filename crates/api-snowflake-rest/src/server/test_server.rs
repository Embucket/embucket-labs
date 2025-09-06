use super::server_models::Config;
use crate::server::router::make_app;
use core_history::store::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn run_test_server(demo_user: &str, demo_password: &str) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBHistoryStore::new(db));

    let snowflake_rest_cfg = Config::new("JSON")
        .expect("Failed to create snowflake config")
        .with_demo_credentials(demo_user.to_string(), demo_password.to_string());

    let app = make_app(metastore, history, snowflake_rest_cfg)
        .await
        .unwrap()
        .into_make_service_with_connect_info::<SocketAddr>();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}
