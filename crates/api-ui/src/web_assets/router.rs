use super::handler::WEB_ASSETS_MOUNT_PATH;
use super::handler::{root_handler, tar_handler};
use axum::{Router, routing::get};
use tower_http::trace::TraceLayer;

pub fn web_assets_app() -> Router {
    Router::new()
        .route(WEB_ASSETS_MOUNT_PATH, get(root_handler))
        .route(
            format!("{WEB_ASSETS_MOUNT_PATH}{{*path}}").as_str(),
            get(tar_handler),
        )
        .layer(TraceLayer::new_for_http())
}
