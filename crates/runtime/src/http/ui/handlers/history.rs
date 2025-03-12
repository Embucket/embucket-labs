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

use crate::http::error::ErrorResponse;
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use axum::response::IntoResponse;
use axum::{extract::Query, extract::State, Json};
use http::status::StatusCode;
use icebucket_history::{store, HistoryItem};
use icebucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Error, Formatter};
use std::time::Instant;
use tracing;
use utoipa::{OpenApi, ToSchema};

pub struct HistoryHandlerError(store::QueryHistoryError);

// for tracing logs
impl Display for HistoryHandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.0.fmt(f)
    }
}

impl IntoResponse for HistoryHandlerError {
    fn into_response(self) -> axum::response::Response {
        let err_code = match self.0 {
            store::QueryHistoryError::QHistoryGet { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            // OK is a stub for us, as this store function has no handler
            store::QueryHistoryError::QHistoryAdd { .. } => StatusCode::OK,
        };
        let er = ErrorResponse {
            message: self.0.to_string(),
            status_code: err_code.as_u16(),
        };
        (err_code, Json(er)).into_response()
    }
}

pub type HistoryResult<T> = Result<T, HistoryHandlerError>;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct GetHistoryItemsParams {
    cursor: Option<String>,
    limit: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct HistoryResponse {
    pub items: Vec<HistoryItem>,
    pub result: String,
    pub duration_seconds: f32,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}

impl HistoryResponse {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(
        items: Vec<HistoryItem>,
        result: String,
        duration_seconds: f32,
        current_cursor: Option<String>,
        next_cursor: String,
    ) -> Self {
        Self {
            items,
            result,
            duration_seconds,
            current_cursor,
            next_cursor,
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        history,
    ),
    components(
        schemas(
            HistoryResponse,
        )
    ),
    tags(
        (name = "history", description = "History access endpoint.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/history",
    params(("cursor" = String, description = "Cursor")),
    params(("limit" = u16, description = "Limit")),
    operation_id = "getHistory",
    tags = ["history"],
    responses(
        (status = 200, description = "Returns result of the history", body = HistoryResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn history(
    DFSessionId(session_id): DFSessionId,
    Query(params): Query<GetHistoryItemsParams>,
    State(state): State<AppState>,
) -> HistoryResult<Json<HistoryResponse>> {
    let start = Instant::now();
    let items = state
        .qhistory
        .query_history(params.cursor.clone(), params.limit)
        .await
        .map_err(HistoryHandlerError)?;
    let next_cursor = if let Some(last_item) = items.last() {
        last_item.next_cursor().to_string()
    } else {
        String::new() // no items in range -> go to beginning
    };
    let duration = start.elapsed();
    Ok(Json(HistoryResponse {
        items,
        result: String::new(),
        duration_seconds: duration.as_secs_f32(), // how much time query history request taken
        current_cursor: params.cursor,
        next_cursor,
    }))
}
