use crate::http::state::AppState;
use axum::routing::post;
use axum::Router;

use super::handlers::{login, refresh_access_token, logout};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh_access_token))
        .route("/auth/logout", post(logout))
}
