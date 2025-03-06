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

use crate::{http::{error::ErrorResponse, ui::error::{self as ui_error, UIError, UIResult}}};
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use validator::Validate;
use std::time::Instant;
use utoipa::{IntoParams, OpenApi, ToSchema};
use icebucket_utils::IterableEntity;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use bytes::Bytes;

// HistoryItem struct is used for storing Query History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryItem {
    pub id: Uuid,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status_code: u16,
}

impl IterableEntity for HistoryItem {
    const SUFFIX_MAX_LEN: usize = 19; //for int64::MAX
    const PREFIX: &[u8] = b"hi.";

    fn key(&self) -> Bytes {
        Self::key_with_prefix(self.start_time.timestamp_nanos_opt().unwrap_or(0))
    }

    fn min_key() -> Bytes {
        Self::key_with_prefix(0)
    }

    fn max_key() -> Bytes {
        Self::key_with_prefix(i64::MAX)
    }
}

#[derive(Deserialize, utoipa::IntoParams)]
struct GetHistoryItemsParams {
    cursor: Option<String>,
    limit: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct HistoryResponse {
    pub items: Vec<HistoryItem>,
    pub result: String,
    pub duration_seconds: f32,
}

impl HistoryResponse {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(items: Vec<HistoryItem>, result: String, duration_seconds: f32) -> Self {
        Self {
            items,
            result,
            duration_seconds,
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
) -> UIResult<Json<HistoryResponse>> {

    let start = Instant::now();
    let items = state.metastore.query_history(params.cursor, params.limit).await;
    // let result = state
    //     .execution_svc
    //     .history_table(&session_id, &request.history, history_context)
    //     .await
    //     .map_err(|e| UIError::Execution { source: e })?;
    let duration = start.elapsed();
    Ok(Json(HistoryResponse {
        items,
        result: String::new(),
        duration_seconds: duration.as_secs_f32(),
    }))
}
