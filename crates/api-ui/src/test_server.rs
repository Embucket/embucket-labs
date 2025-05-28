use crate::auth::layer::require_auth;
use crate::auth::router as auth_router;
use crate::layers::make_cors_middleware;
use crate::router;
use crate::state;
use crate::{config::AuthConfig, config::WebConfig};
use api_sessions::{RequestSessionMemory, RequestSessionStore};
use axum::Router;
use axum::middleware;
use core_executor::service::CoreExecutionService;
use core_executor::utils::DataSerializationFormat;
use core_history::RecordingExecutionService;
use core_history::store::SlateDBWorksheetsStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;
use time::Duration;
use tower_sessions::{Expiry, SessionManagerLayer};

#[allow(clippy::unwrap_used)]
pub async fn run_test_server_with_demo_auth(
    jwt_secret: String,
    demo_user: String,
    demo_password: String,
) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history = Arc::new(SlateDBWorksheetsStore::new(db));
    let mut auth_config = AuthConfig::new(jwt_secret);
    auth_config.with_demo_credentials(demo_user, demo_password);

    let app = make_app(
        metastore,
        history,
        &WebConfig {
            port: 3000,
            host: "0.0.0.0".to_string(),
            allow_origin: None,
        },
        auth_config,
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

#[allow(clippy::unwrap_used)]
pub async fn run_test_server() -> SocketAddr {
    run_test_server_with_demo_auth(String::new(), String::new(), String::new()).await
}

#[allow(clippy::needless_pass_by_value)]
pub fn make_app(
    metastore: Arc<SlateDBMetastore>,
    history_store: Arc<SlateDBWorksheetsStore>,
    config: &WebConfig,
    auth_config: AuthConfig,
) -> Result<Router, Box<dyn std::error::Error>> {
    let execution_svc = Arc::new(CoreExecutionService::new(metastore.clone()));
    let execution_svc = Arc::new(RecordingExecutionService::new(
        execution_svc,
        history_store.clone(),
        DataSerializationFormat::Json,
    ));
    let session_memory = RequestSessionMemory::default();
    let session_store = RequestSessionStore::new(session_memory, execution_svc.clone());
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(5 * 60)));

    // Create the application state
    let app_state = state::AppState::new(
        metastore,
        history_store,
        execution_svc,
        Arc::new(config.clone()),
        Arc::new(auth_config),
    );

    let ui_router = router::create_router().with_state(app_state.clone());
    let ui_router = ui_router.layer(middleware::from_fn_with_state(
        app_state.clone(),
        require_auth,
    ));
    let mut router = Router::new()
        .nest("/ui", ui_router)
        .nest(
            "/ui/auth",
            auth_router::create_router().with_state(app_state),
        )
        .layer(session_layer);

    if let Some(allow_origin) = config.allow_origin.as_ref() {
        router = router.layer(make_cors_middleware(allow_origin));
    }

    Ok(router)
}
