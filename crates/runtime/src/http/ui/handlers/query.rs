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

use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use crate::{
    execution::query::IceBucketQueryContext,
    http::{
        error::ErrorResponse,
        ui::error::{UIError, UIResult},
    },
};
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Instant};
use utoipa::{OpenApi, ToSchema};
use validator::Validate;

// Temporarily copied here
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryPayload {
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

impl QueryPayload {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(query: String) -> Self {
        Self {
            query,
            context: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub query: String,
    pub result: String,
    pub duration_seconds: f32,
}

impl QueryResponse {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(query: String, result: String, duration_seconds: f32) -> Self {
        Self {
            query,
            result,
            duration_seconds,
        }
    }
}

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
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn query(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(payload): Json<QueryPayload>,
) -> UIResult<Json<QueryResponse>> {
    let request: QueryPayload = payload;
    let query_context = IceBucketQueryContext {
        database: request
            .context
            .as_ref()
            .and_then(|c| c.get("database").cloned()),
        schema: request
            .context
            .as_ref()
            .and_then(|c| c.get("schema").cloned()),
    };

    let start = Instant::now();
    let result = state
        .execution_svc
        .query_table(&session_id, &request.query, query_context)
        .await
        .map_err(|e| UIError::Execution { source: e })?;
    let duration = start.elapsed();
    Ok(Json(QueryResponse {
        query: request.query.clone(),
        result,
        duration_seconds: duration.as_secs_f32(),
    }))
}
