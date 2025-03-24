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
use crate::http::ui::worksheets::{
    errors::{WorksheetsAPIError, WorksheetsResult},
    WorksheetPayload, WorksheetResponse, WorksheetsResponse,
};
use axum::{
    extract::{Path, State},
    Json,
};
use icebucket_history::{Worksheet, WorksheetId};
use tracing;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        worksheets,
        worksheet,
        create_worksheet,
        delete_worksheet,
        update_worksheet,
    ),
    components(schemas(ErrorResponse, WorksheetPayload, WorksheetResponse, WorksheetsResponse,))
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/worksheets",
    operation_id = "getWorksheets",
    tags = ["worksheets"],
    responses(
        (status = 200, description = "Get list of worksheets", body = WorksheetsResponse),
        (status = 400, description = "Unknown", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn worksheets(
    State(state): State<AppState>,
) -> WorksheetsResult<Json<WorksheetsResponse>> {
    let items = state
        .history
        .get_worksheets()
        .await
        .map_err(|e| WorksheetsAPIError::List { source: e })?;
    Ok(Json(WorksheetsResponse { items }))
}

#[utoipa::path(
    post,
    path = "/ui/worksheets",
    operation_id = "createWorksheet",
    tags = ["worksheets"],
    request_body(
        content(
            (
                WorksheetPayload = "application/json", 
                examples (
                    ("with name" = (
                        value = json!(WorksheetPayload {
                            name: Some("worksheet1".to_string()), 
                            content: Some("select 1".to_string()),
                        })
                    )),
                    ("content only" = (
                        value = json!(WorksheetPayload {
                            name: None,
                            content: Some("select 1".to_string()),
                        })
                    )),
                    ("empty" = (
                        value = json!(WorksheetPayload {
                            name: None,
                            content: None,
                        })
                    )),
                )
            ),
        )
    ),
    responses(
        (status = 200, description = "Created worksheet", body = WorksheetResponse),
        (status = 409, description = "Already Exists", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn create_worksheet(
    State(state): State<AppState>,
    Json(payload): Json<WorksheetPayload>,
) -> WorksheetsResult<Json<WorksheetResponse>> {
    let request = Worksheet::new(payload.content);
    let worksheet = state
        .history
        .add_worksheet(request)
        .await
        .map_err(|e| WorksheetsAPIError::Create { source: e })?;
    Ok(Json(WorksheetResponse { data: worksheet }))
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
        (status = 400, description = "Bad request", body = ErrorResponse),        
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetsResult<Json<WorksheetResponse>> {
    let worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Get { source: e })?;
    Ok(Json(WorksheetResponse { data: worksheet }))
}

#[utoipa::path(
    delete,
    path = "/ui/worksheets/{worksheet_id}",
    operation_id = "deleteWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Worksheet deleted"),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// Add time sql took
pub async fn delete_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetsResult<()> {
    state
        .history
        .delete_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Delete { source: e })
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
        (status = 400, description = "Bad request", body = ErrorResponse),
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
) -> WorksheetsResult<()> {
    let mut worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Update { source: e })?;

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
        .map_err(|e| WorksheetsAPIError::Update { source: e })
}
