use crate::auth::layer::require_auth;
use crate::auth::router as auth_router;
use crate::layers::make_cors_middleware;
use crate::router;
use crate::state;
use crate::{config::AuthConfig, config::WebConfig};
use api_sessions::layer::propagate_session_cookie;
use api_sessions::session::{SESSION_EXPIRATION_SECONDS, SessionStore};
use axum::Router;
use axum::middleware;
use core_executor::service::CoreExecutionService;
use core_executor::utils::Config;
use core_history::store::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use core_utils::Db;
use std::net::SocketAddr;
use std::sync::Arc;

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
    let history = Arc::new(SlateDBHistoryStore::new(db));
    let auth_config = AuthConfig::new(jwt_secret).with_demo_credentials(demo_user, demo_password);

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
    run_test_server_with_demo_auth(String::new(), String::new(), String::new()).await
}

#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub async fn make_app(
    metastore: Arc<SlateDBMetastore>,
    history_store: Arc<SlateDBHistoryStore>,
    config: &WebConfig,
    auth_config: AuthConfig,
) -> Result<Router, Box<dyn std::error::Error>> {
    history_store.init().await?;

    let execution_svc = Arc::new(
        CoreExecutionService::new(
            metastore.clone(),
            history_store.clone(),
            Arc::new(Config::default().with_bootstrap_default_entities(false)),
        )
        .await
        .expect("Failed to create execution service"),
    );

    // Create the application state
    let app_state = state::AppState::new(
        metastore,
        history_store,
        execution_svc.clone(),
        Arc::new(config.clone()),
        Arc::new(auth_config),
    );

    let session_store = SessionStore::new(execution_svc);

    tokio::task::spawn({
        let session_store = session_store.clone();
        async move {
            session_store
                .continuously_delete_expired(tokio::time::Duration::from_secs(
                    SESSION_EXPIRATION_SECONDS,
                ))
                .await;
        }
    });

    let ui_router = router::create_router().with_state(app_state.clone()).layer(
        middleware::from_fn_with_state(session_store, propagate_session_cookie),
    );
    let ui_router = ui_router.layer(middleware::from_fn_with_state(
        app_state.clone(),
        require_auth,
    ));
    let mut router = Router::new().nest("/ui", ui_router).nest(
        "/ui/auth",
        auth_router::create_router().with_state(app_state),
    );

    if let Some(allow_origin) = config.allow_origin.as_ref() {
        router = router.layer(make_cors_middleware(allow_origin));
    }

    Ok(router)
}
