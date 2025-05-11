use crate::state::AppState;
use axum::Router;
use axum::routing::{get, post};

use super::handlers::{account, login, logout, refresh_access_token};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/refresh", post(refresh_access_token))
        .route("/logout", post(logout))
        .route("/account", get(account))
}
