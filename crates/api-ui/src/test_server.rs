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
use core_history::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use std::net::SocketAddr;
use tokio::runtime::Builder;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub fn run_test_server_with_demo_auth(
    jwt_secret: String,
    demo_user: String,
    demo_password: String,
) -> SocketAddr {

    let server_cond = Arc::new((Mutex::new(false), Condvar::new())); // Shared state with a condition 
    let server_cond_clone = Arc::clone(&server_cond);

    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Start a new thread for the server
    let _handle = std::thread::spawn(move || {
        // Create the Tokio runtime
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        // Start the Axum server
        rt.block_on(async move {
            let metastore = SlateDBMetastore::new_in_memory().await;
            let history = SlateDBHistoryStore::new_in_memory().await;
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

            // Lock the mutex and set the notification flag
            {
                let (lock, cvar) = &*server_cond_clone;
                let mut notify_server_started = lock.lock().unwrap();
                *notify_server_started = true; // Set notification
                cvar.notify_one(); // Notify the waiting thread
            }
                        
            // Serve the application
            axum_server::from_tcp(listener)
                .serve(app)
                .await
                .unwrap();
        });
    });
    // Note: Not joining thread as
    // We are not interested in graceful thread termination, as soon out tests passed.

    let (lock, cvar) = &*server_cond;
    let timeout_duration = std::time::Duration::from_secs(1);

    // Lock the mutex and wait for notification with timeout
    let notified = lock.lock().unwrap();
    let result = cvar.wait_timeout(notified, timeout_duration).unwrap();

    // Check if notified or timed out
    if !*result.0 {
        tracing::error!("Timeout occurred while waiting for server start.");
    } else {
        tracing::info!("Test server is up and running.");
        std::thread::sleep(Duration::from_millis(10));
    }

    addr
}

#[allow(clippy::unwrap_used)]
pub fn run_test_server() -> SocketAddr {
    run_test_server_with_demo_auth(String::new(), String::new(), String::new())
}

#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub async fn make_app(
    metastore: SlateDBMetastore,
    history_store: SlateDBHistoryStore,
    config: &WebConfig,
    auth_config: AuthConfig,
) -> Result<Router, Box<dyn std::error::Error>> {
    let metastore = Arc::new(metastore);
    let history_store = Arc::new(history_store);
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
