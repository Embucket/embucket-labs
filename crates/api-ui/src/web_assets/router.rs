use crate::state::AppState;
use axum::Router;
use axum::routing::get;

use crate::web_assets::handlers::{root_handler, tar_handler};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/", get(root_handler))
        .route(format!("/{{*path}}").as_str(), get(tar_handler))
}
