use super::server_models::Config;
use crate::server::router::make_app;
use crate::server::server_models::Config as AppCfg;
use core_executor::utils::Config as UtilsConfig;
use core_history::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use tracing_subscriber::fmt::format::FmtSpan;
use tokio::runtime::Builder;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, Condvar};

pub fn server_default_cfg(data_format: &str) -> Option<(AppCfg, UtilsConfig)> {
    Some((
        Config::new(data_format)
            .expect("Failed to create server config")
            .with_demo_credentials("embucket".to_string(), "embucket".to_string()),
        UtilsConfig::default().with_max_concurrency_level(2),
    ))
}

#[allow(clippy::expect_used)]
pub fn run_test_rest_api_server(server_cfg: Option<(AppCfg, UtilsConfig)>) -> SocketAddr {
    let (app_cfg, executor_cfg) = server_cfg.unwrap_or_else(|| {
        server_default_cfg("json").unwrap()
    });

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
        rt.block_on(async {
            let _ = run_test_rest_api_server_with_config(app_cfg, executor_cfg, listener, server_cond_clone).await;
        });
    });
    // Note: Not joining thread as
    // We are not interested in graceful thread termination, as soon out tests passed.

    let (lock, cvar) = &*server_cond;
    let timeout_duration = std::time::Duration::from_secs(1);

    // Lock the mutex and wait for notification with timeout
    let mut notified = lock.lock().unwrap();
    let result = cvar.wait_timeout(notified, timeout_duration).unwrap();

    // Check if notified or timed out
    if !*result.0 {
        tracing::error!("Timeout occurred while waiting for server start.");
    } else {
        tracing::info!("Test server is up and running.");
        thread::sleep(Duration::from_millis(10));
    }

    addr
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn run_test_rest_api_server_with_config(
    app_cfg: Config,
    execution_cfg: UtilsConfig,
    listener: std::net::TcpListener,
    server_cond: Arc<(Mutex<bool>, Condvar)>,
) {
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
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .finish();

    // ignoring error: as with parralel tests execution, just first thread is able to set it successfully
    // since all tests run in a single process
    let _ = tracing::subscriber::set_global_default(subscriber);

    tracing::info!("Starting server at {}", addr);

    let metastore = SlateDBMetastore::new_in_memory().await;
    let history = SlateDBHistoryStore::new_in_memory().await;

    let app = make_app(metastore, history, app_cfg, execution_cfg)
        .await
        .unwrap()
        .into_make_service_with_connect_info::<SocketAddr>();

    // Lock the mutex and set the notification flag
    {
        let (lock, cvar) = &*server_cond;
        let mut notify_server_started = lock.lock().unwrap();
        *notify_server_started = true; // Set notification
        cvar.notify_one(); // Notify the waiting thread
    }
    
    tracing::info!("Server ready at {}", addr);

    // Serve the application
    axum_server::from_tcp(listener)
        .serve(app)
        .await
        .unwrap();
}
