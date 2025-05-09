use crate::http::state::AppState;
use axum::routing::{get, post};
use axum::Router;

use super::handlers::{account, login, logout, refresh_access_token};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh_access_token))
        .route("/auth/logout", post(logout))
        .route("/account", get(account))
}
