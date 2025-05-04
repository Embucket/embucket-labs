use crate::http::state::AppState;
use axum::routing::{get, post};
use axum::Router;

use super::handlers::{login, refresh_access_token};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/refresh", get(refresh_access_token))
}
