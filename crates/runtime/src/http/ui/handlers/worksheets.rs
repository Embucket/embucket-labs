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
use crate::http::state::AppState;
use crate::http::ui::models::{
    history::HistoryResponse,
    worksheet::{WorksheetPayload, WorksheetResponse, WorksheetsResponse},
};
use axum::response::IntoResponse;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use http::status::StatusCode;
use icebucket_history::{store, QueryHistoryItem, Worksheet, WorksheetId};
use icebucket_utils::iterable::IterableEntity;
use serde::Deserialize;
use std::fmt::{Display, Error, Formatter};
use std::time::Instant;
use tracing;
use utoipa::OpenApi;

pub struct WorksheetHandlerError(store::WorksheetsStoreError);

// for tracing logs
impl Display for WorksheetHandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.0.fmt(f)
    }
}

impl IntoResponse for WorksheetHandlerError {
    fn into_response(self) -> axum::response::Response {
        let err_code = match self.0 {
            store::WorksheetsStoreError::WorksheetDelete { .. }
            | store::WorksheetsStoreError::WorksheetUpdate { .. }
            | store::WorksheetsStoreError::HistoryGet { .. }
            | store::WorksheetsStoreError::WorksheetGet { .. }
            | store::WorksheetsStoreError::WorksheetAdd { .. }
            | store::WorksheetsStoreError::BadKey { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            // OK is a stub for us, as this store function has no handler
            store::WorksheetsStoreError::WorksheetNotFound { .. } => StatusCode::NOT_FOUND,
            store::WorksheetsStoreError::HistoryAdd { .. } => StatusCode::OK,
        };
        let er = ErrorResponse {
            message: self.0.to_string(),
            status_code: err_code.as_u16(),
        };
        (err_code, Json(er)).into_response()
    }
}

pub type WorksheetHandlerResult<T> = Result<T, WorksheetHandlerError>;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct GetHistoryItemsParams {
    cursor: Option<i64>,
    limit: Option<u16>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        worksheets,
        create_worksheet,
        worksheet,
        delete_worksheet,
        update_worksheet,
        history,
    ),
    components(schemas(
        ErrorResponse,
        HistoryResponse,
        WorksheetPayload,
        WorksheetResponse,
        WorksheetsResponse,
    ))
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/worksheets",
    operation_id = "getWorksheets",
    tags = ["worksheets"],
    responses(
        (status = 200, description = "Returns list of worksheets", body = WorksheetsResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn worksheets(
    State(state): State<AppState>,
) -> WorksheetHandlerResult<Json<WorksheetsResponse>> {
    let start = Instant::now();
    let items = state
        .history
        .get_worksheets()
        .await
        .map_err(WorksheetHandlerError)?;
    let duration = start.elapsed();
    Ok(Json(WorksheetsResponse {
        data: items,
        duration_seconds: duration.as_secs_f32(),
    }))
}

#[utoipa::path(
    post,
    path = "/ui/worksheets",
    operation_id = "createWorksheet",
    tags = ["worksheets"],
    responses(
        (status = 200, description = "Created worksheet", body = WorksheetResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn create_worksheet(
    State(state): State<AppState>,
    Json(payload): Json<WorksheetPayload>,
) -> WorksheetHandlerResult<Json<WorksheetResponse>> {
    let request = Worksheet::new(payload.content);
    let start = Instant::now();
    let worksheet = state
        .history
        .add_worksheet(request)
        .await
        .map_err(WorksheetHandlerError)?;
    let duration = start.elapsed();
    Ok(Json(WorksheetResponse {
        data: Some(worksheet),
        duration_seconds: duration.as_secs_f32(),
    }))
}

#[utoipa::path(
    get,
    path = "/ui/worksheets/{worksheet_id}",
    operation_id = "getWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Returns worksheet", body = WorksheetResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetHandlerResult<Json<WorksheetResponse>> {
    let start = Instant::now();
    let worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(WorksheetHandlerError)?;
    let duration = start.elapsed();
    Ok(Json(WorksheetResponse {
        data: Some(worksheet),
        duration_seconds: duration.as_secs_f32(),
    }))
}

#[utoipa::path(
    delete,
    path = "/ui/worksheets/{worksheet_id}",
    operation_id = "delWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Worksheet deleted", body = ()),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn delete_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetHandlerResult<Json<WorksheetResponse>> {
    let start = Instant::now();
    state
        .history
        .delete_worksheet(worksheet_id)
        .await
        .map_err(WorksheetHandlerError)?;
    let duration = start.elapsed();
    Ok(Json(WorksheetResponse {
        data: None,
        duration_seconds: duration.as_secs_f32(),
    }))
}

#[utoipa::path(
    patch,
    path = "/ui/worksheets/{worksheet_id}",
    operation_id = "updateWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Worksheet updated", body = WorksheetResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn update_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
    Json(payload): Json<WorksheetPayload>,
) -> WorksheetHandlerResult<Json<WorksheetResponse>> {
    let start = Instant::now();

    let mut worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(WorksheetHandlerError)?;

    if let Some(name) = payload.name {
        worksheet.set_name(name);
    }

    if let Some(content) = payload.content {
        worksheet.set_content(content);
    }

    state
        .history
        .update_worksheet(worksheet)
        .await
        .map_err(WorksheetHandlerError)?;
    let duration = start.elapsed();
    Ok(Json(WorksheetResponse {
        data: None,
        duration_seconds: duration.as_secs_f32(), // how much time query history request taken
    }))
}

#[utoipa::path(
    get,
    path = "/worksheets/{worksheet_id}/queries",
    params(("cursor" = String, description = "Cursor")),
    params(("limit" = u16, description = "Limit")),
    operation_id = "getQueriesHistory",
    tags = ["queries"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Returns result of the history", body = HistoryResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn history(
    Query(params): Query<GetHistoryItemsParams>,
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetHandlerResult<Json<HistoryResponse>> {
    let start = Instant::now();
    let items = state
        .history
        .query_history(worksheet_id, params.cursor, params.limit)
        .await
        .map_err(WorksheetHandlerError)?;
    let next_cursor = if let Some(last_item) = items.last() {
        last_item.next_cursor()
    } else {
        QueryHistoryItem::min_cursor() // no items in range -> go to beginning
    };
    let duration = start.elapsed();
    Ok(Json(HistoryResponse {
        items,
        duration_seconds: duration.as_secs_f32(),
        current_cursor: params.cursor,
        next_cursor,
    }))
}
