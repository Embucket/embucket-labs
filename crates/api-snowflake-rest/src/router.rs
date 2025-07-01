use crate::handlers::{abort, query};
use crate::state::AppState;
use axum::Router;
use axum::routing::post;

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/queries/v1/query-request", post(query))
        .route("/queries/v1/abort-request", post(abort))
}
