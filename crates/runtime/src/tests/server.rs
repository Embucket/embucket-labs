use crate::http::{config::WebConfig, make_app};
use embucket_history::store::SlateDBWorksheetsStore;
use embucket_metastore::SlateDBMetastore;
use embucket_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;

#[allow(clippy::unwrap_used)]
pub async fn run_test_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBWorksheetsStore::new(db));

    let app = make_app(
        metastore,
        history,
        &WebConfig {
            port: 3000,
            host: "0.0.0.0".to_string(),
            allow_origin: None,
            data_format: "json".to_string(),
            iceberg_catalog_url: "http://127.0.0.1".to_string(),
        },
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}
