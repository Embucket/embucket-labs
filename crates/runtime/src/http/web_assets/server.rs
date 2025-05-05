use super::config::StaticWebConfig;
use super::handler::WEB_ASSETS_MOUNT_PATH;
use super::handler::{root_handler, tar_handler};
use crate::http::layers::make_cors_middleware;
use axum::{routing::get, Router};
use core::net::SocketAddr;
use tower_http::trace::TraceLayer;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_web_assets_server(
    config: &StaticWebConfig,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let StaticWebConfig {
        host,
        port,
        allow_origin,
    } = config;

    let mut app = Router::new()
        .route(WEB_ASSETS_MOUNT_PATH, get(root_handler))
        .route(
            format!("{WEB_ASSETS_MOUNT_PATH}{{*path}}").as_str(),
            get(tar_handler),
        )
        .layer(TraceLayer::new_for_http());

    if let Some(allow_origin) = allow_origin.as_ref() {
        app = app.layer(make_cors_middleware(allow_origin)?);
    }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    tracing::info!("Listening on http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app)
            // .with_graceful_shutdown(shutdown_signal())
            .await
    });

    Ok(addr)
}
