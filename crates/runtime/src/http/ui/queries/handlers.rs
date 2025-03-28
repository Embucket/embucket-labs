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
use crate::http::ui::queries::models::{
    ExecutionContext, GetHistoryItemsParams, QueriesResponse, QueryCreatePayload,
    QueryCreateResponse,
};
use crate::http::{
    error::ErrorResponse,
    ui::queries::error::{QueriesAPIError, QueriesResult, QueryError},
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use icebucket_history::{QueryRecord, QueryRecordId, WorksheetId};
use icebucket_utils::iterable::IterableEntity;
use std::collections::HashMap;
use std::time::Instant;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(query, history,),
    components(schemas(QueriesResponse, QueryCreateResponse, QueryCreatePayload,))
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/worksheets/{worksheet_id}/queries",
    operation_id = "createQuery",
    tags = ["queries"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id")
    ),
    request_body(
        content(
            (
                QueryCreatePayload = "application/json", 
                examples (
                    ("with context" = (
                        value = json!(QueryCreatePayload {
                            query: "CREATE TABLE test(a INT);".to_string(),
                            context: Some(HashMap::from([
                                ("database".to_string(), "my_database".to_string()),
                                ("schema".to_string(), "public".to_string()),
                            ])),
                        })
                    )),
                )
            ),
        )
    ),
    responses(
        (status = 200, description = "Returns result of the query", body = QueryCreateResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 409, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
    Json(request): Json<QueryCreatePayload>,
) -> QueriesResult<Json<QueryCreateResponse>> {
    let worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| QueriesAPIError::Query {
            source: QueryError::Store { source: e },
        })?;

    let query_context = ExecutionContext {
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
    let (id, result) = state
        .execution_svc
        .query_table(&session_id, worksheet, &request.query, query_context)
        .await
        .map_err(|e| QueriesAPIError::Query {
            source: QueryError::Execution { source: e },
        })?;
    let duration = start.elapsed();
    Ok(Json(QueryCreateResponse {
        id,
        worksheet_id,
        query: request.query.clone(),
        result,
        duration_seconds: duration.as_secs_f32(),
    }))
}

#[utoipa::path(
    get,
    path = "/worksheets/{worksheet_id}/queries",
    operation_id = "getQueries",
    tags = ["queries"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id"),
        ("cursor" = Option<QueryRecordId>, Query, description = "Cursor"),
        ("limit" = Option<u16>, Query, description = "History items limit"),
    ),
    responses(
        (status = 200, description = "Returns queries history", body = QueriesResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 400, description = "Bad worksheet key", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn history(
    Query(params): Query<GetHistoryItemsParams>,
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> QueriesResult<Json<QueriesResponse>> {
    // check if worksheet is exists (get and waste entire worksheet)
    state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| QueriesAPIError::Queries { source: e })?;

    let items = state
        .history
        .query_history(worksheet_id, params.cursor, params.limit)
        .await
        .map_err(|e| QueriesAPIError::Queries { source: e })?;
    let next_cursor = if let Some(last_item) = items.last() {
        last_item.next_cursor()
    } else {
        QueryRecord::min_cursor() // no items in range -> go to beginning
    };
    Ok(Json(QueriesResponse {
        items,
        current_cursor: params.cursor,
        next_cursor,
    }))
}
