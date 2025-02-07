use super::super::models::error::{self as model_error, NexusError, NexusResult};
use crate::http::ui::models::table::{QueryPayload, QueryResponse};
use crate::state::AppState;
use axum::{extract::State, Json};
use snafu::ResultExt;
use std::time::Instant;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        query,
    ),
    components(
        schemas(
            QueryResponse,
            QueryPayload,
        )
    ),
    tags(
        (name = "query", description = "Query management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/ui/query",
    request_body = QueryPayload,
    operation_id = "query",
    tags = ["query"],
    responses(
        (status = 200, description = "Returns result of the query", body = QueryResponse),
        (status = 422, description = "Unprocessable entity", body = NexusError),
        (status = 500, description = "Internal server error", body = NexusError)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn query(
    State(state): State<AppState>,
    Json(payload): Json<QueryPayload>,
) -> NexusResult<Json<QueryResponse>> {
    let request: QueryPayload = payload;
    let start = Instant::now();
    let result = state
        .control_svc
        .query_table(&request.query)
        .await
        .context(model_error::QuerySnafu)?;
    let duration = start.elapsed();
    Ok(Json(QueryResponse {
        query: request.query.clone(),
        result,
        duration_seconds: duration.as_secs_f32(),
    }))
}
