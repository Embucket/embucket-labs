use crate::http::state::AppState;
use axum::routing::post;
use axum::Router;

use super::handlers::login;

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
}
