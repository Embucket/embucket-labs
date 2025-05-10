use crate::handlers::{abort, login, query};
use crate::state::AppState;
use axum::Router;
use axum::routing::post;

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/session/v1/login-request", post(login))
        .route("/queries/v1/query-request", post(query))
        .route("/queries/v1/abort-request", post(abort))
}
