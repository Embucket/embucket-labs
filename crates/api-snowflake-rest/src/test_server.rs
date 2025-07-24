use crate::auth::create_router as create_auth_router;
use crate::layer::require_auth;
use crate::router::create_router;
use crate::schemas::Config;
use crate::state;
use axum::Router;
use axum::middleware;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config as UtilsConfig;
use core_history::store::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn run_test_server_with_demo_auth(
    demo_user: String,
    demo_password: String,
) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBHistoryStore::new(db));

    let snowflake_rest_cfg = Config::new("JSON")
        .expect("Failed to create snowflake config")
        .with_demo_credentials(demo_user, demo_password);

    let app = make_app(metastore, history, snowflake_rest_cfg)
        .await
        .unwrap()
        .into_make_service_with_connect_info::<SocketAddr>();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

#[allow(clippy::unwrap_used)]
pub async fn run_test_server() -> SocketAddr {
    run_test_server_with_demo_auth(String::new(), String::new()).await
}

#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub async fn make_app(
    metastore: Arc<SlateDBMetastore>,
    history_store: Arc<SlateDBHistoryStore>,
    snowflake_rest_cfg: Config,
) -> Result<Router, Box<dyn std::error::Error>> {
    let execution_svc = Arc::new(
        CoreExecutionService::new(metastore, history_store, Arc::new(UtilsConfig::default()))
            .await
            .expect("Failed to create execution service"),
    );

    // Create the application state

    let snowflake_state = state::AppState {
        execution_svc,
        config: snowflake_rest_cfg,
    };

    let snowflake_router =
        create_router()
            .with_state(snowflake_state.clone())
            .layer(middleware::from_fn_with_state(
                snowflake_state.clone(),
                require_auth,
            ));
    let snowflake_auth_router = create_auth_router().with_state(snowflake_state);
    let snowflake_router = snowflake_router.merge(snowflake_auth_router);

    let router = Router::new().merge(snowflake_router);

    Ok(router)
}
