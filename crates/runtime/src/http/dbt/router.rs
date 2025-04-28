use crate::http::state::AppState;
use axum::routing::post;
use axum::Router;

use crate::http::dbt::handlers::{abort, login, query};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/session/v1/login-request", post(login))
        .route("/queries/v1/query-request", post(query))
        .route("/queries/v1/abort-request", post(abort))
}
