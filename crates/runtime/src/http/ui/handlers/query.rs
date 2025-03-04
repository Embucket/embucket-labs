// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::models::error::{self as model_error, NexusError, NexusResult};
use crate::http::session::DFSessionId;
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
    operation_id = "runQuery",
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
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(payload): Json<QueryPayload>,
) -> NexusResult<Json<QueryResponse>> {
    let request: QueryPayload = payload;
    let start = Instant::now();
    let result = state
        .control_svc
        .query_table(&session_id, &request.query)
        .await
        .context(model_error::QuerySnafu)?;
    let duration = start.elapsed();
    Ok(Json(QueryResponse {
        query: request.query.clone(),
        result,
        duration_seconds: duration.as_secs_f32(),
    }))
}
