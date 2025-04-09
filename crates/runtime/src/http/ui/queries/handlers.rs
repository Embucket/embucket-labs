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
    ExecutionContext, GetQueriesParams, PostQueriesParams, QueriesResponse, QueryCreatePayload,
    QueryCreateResponse, QueryRecord, ResultSet,
};
use crate::http::{
    error::ErrorResponse,
    ui::queries::error::{QueriesAPIError, QueriesResult, QueryError},
};
use axum::{
    extract::{Query, State},
    Json,
};
use icebucket_history::{QueryRecord as QueryRecordItem, QueryRecordId, WorksheetId};
use icebucket_utils::iterable::IterableEntity;
use std::collections::HashMap;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(query, queries),
    components(schemas(QueriesResponse, QueryCreateResponse, QueryCreatePayload,)),
    tags(
      (name = "queries", description = "Queries endpoints"),
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/queries",
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
        (status = 409, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Query(params): Query<PostQueriesParams>,
    Json(request): Json<QueryCreatePayload>,
) -> QueriesResult<Json<QueryCreateResponse>> {
    //
    // Note: This handler allowed to return error from a designated place only,
    // after query record successfull saved result or error.

    // Here we use worksheet_id = 0 if worksheet_id is not defined,
    // we still can get queries records for non existing worksheet
    let worksheet_id = params.worksheet_id.unwrap_or_default();

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

    // TODO: save query record even if no related worksheet
    let mut query_record = QueryRecordItem::query_start(worksheet_id, &request.query, None);

    let query_res = state
        .execution_svc
        .query(&session_id, &request.query, query_context)
        .await;

    match query_res {
        Ok((ref records, ref columns)) => {
            let result_set = ResultSet::query_result_to_result_set(records, columns);
            match result_set {
                Ok(result_set) => {
                    let encoded_res = serde_json::to_string(&result_set);

                    if let Ok(encoded_res) = encoded_res {
                        let result_count = i64::try_from(records.len()).unwrap_or(0);
                        query_record.query_finished(result_count, Some(encoded_res), None);
                    }
                    // failed to wrap query results
                    else if let Err(err) = encoded_res {
                        query_record.query_finished_with_error(err.to_string());
                    }
                }
                // error getting result_set
                Err(err) => {
                    query_record.query_finished_with_error(err.to_string());
                }
            }
        }
        // query error
        Err(ref err) => {
            // query execution error
            query_record.query_finished_with_error(err.to_string());
        }
    };

    // add query record
    if let Err(err) = state.history.add_query(&query_record).await {
        // do not raise error, just log ?
        tracing::error!("{err}");
    }

    if let Err(err) = query_res {
        Err(QueriesAPIError::Query {
            source: QueryError::Execution { source: err },
        })
    } else {
        Ok(Json(QueryCreateResponse {
            data: QueryRecord::try_from(query_record)
                .map_err(|e| QueriesAPIError::Query { source: e })?,
        }))
    }
}

#[utoipa::path(
    get,
    path = "/queries",
    operation_id = "getQueries",
    tags = ["queries"],
    params(
        ("worksheet_id" = WorksheetId, Path, description = "Worksheet id"),
        ("cursor" = Option<QueryRecordId>, Query, description = "Cursor"),
        ("limit" = Option<u16>, Query, description = "Queries limit"),
    ),
    responses(
        (status = 200, description = "Returns queries history", body = QueriesResponse),
        (status = 400, description = "Bad worksheet key", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn queries(
    Query(params): Query<GetQueriesParams>,
    State(state): State<AppState>,
) -> QueriesResult<Json<QueriesResponse>> {
    // Here we use worksheet_id = 0 if worksheet_id is not defined,
    // we still can get queries records for non existing worksheet
    let worksheet_id = params.worksheet_id.unwrap_or_default();

    let result = state
        .history
        .get_queries(worksheet_id, params.cursor, params.limit)
        .await;

    match result {
        Ok(recs) => {
            let next_cursor = if let Some(last_item) = recs.last() {
                last_item.next_cursor()
            } else {
                QueryRecordItem::min_cursor() // no items in range -> go to beginning
            };
            let queries: Vec<QueryRecord> = recs
                .clone()
                .into_iter()
                .map(QueryRecord::try_from)
                .filter_map(Result::ok)
                .collect();

            let queries_failed_to_load: Vec<QueryError> = recs
                .into_iter()
                .map(QueryRecord::try_from)
                .filter_map(Result::err)
                .collect();
            if !queries_failed_to_load.is_empty() {
                // TODO: fix tracing output
                tracing::error!("Queries: failed to load queries: {queries_failed_to_load:?}");
            }

            Ok(Json(QueriesResponse {
                items: queries,
                current_cursor: params.cursor,
                next_cursor,
            }))
        }
        Err(e) => Err(QueriesAPIError::Queries {
            source: QueryError::Store { source: e },
        }),
    }
}
