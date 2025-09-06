use super::handlers::{abort, get_query, query};
use super::state::AppState;
use axum::Router;
use axum::routing::{get, post};

pub fn create_router() -> Router<AppState> {
    Router::new()
        .route("/queries/v1/query-request", post(query))
        .route("/queries/v1/abort-request", post(abort))
        .route("/queries/{queryId}/result", get(get_query))
}
