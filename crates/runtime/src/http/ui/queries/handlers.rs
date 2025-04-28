use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use crate::http::ui::queries::models::{
    ExecutionContext, GetQueriesParams, QueriesResponse, QueryCreatePayload, QueryCreateResponse,
    QueryRecord, ResultSet,
};
use crate::http::{
    error::ErrorResponse,
    ui::queries::error::{QueriesAPIError, QueriesResult, QueryError},
};
use axum::{
    extract::{Query, State},
    Json,
};
use embucket_history::{QueryRecordActions, QueryRecordId, WorksheetId};
use embucket_utils::iterable::IterableEntity;
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
    path = "/ui/queries",
    operation_id = "createQuery",
    tags = ["queries"],
    request_body(
        content(
            (
                QueryCreatePayload = "application/json", 
                examples (
                    ("with context" = (
                        value = json!(QueryCreatePayload {
                            worksheet_id: None,
                            query: "CREATE TABLE test(a INT);".to_string(),
                            context: Some(HashMap::from([
                                ("database".to_string(), "my_database".to_string()),
                                ("schema".to_string(), "public".to_string()),
                            ])),
                        })
                    )),
                    ("with fully qualified name" = (
                        value = json!(QueryCreatePayload {
                            worksheet_id: None,
                            query: "CREATE TABLE my_database.public.test(a INT);".to_string(),
                            context: None,
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
    Json(payload): Json<QueryCreatePayload>,
) -> QueriesResult<Json<QueryCreateResponse>> {
    //
    // Note: This handler allowed to return error from a designated place only,
    // after query record successfuly saved result or error.

    let query_context = ExecutionContext {
        database: payload
            .context
            .as_ref()
            .and_then(|c| c.get("database").cloned()),
        schema: payload
            .context
            .as_ref()
            .and_then(|c| c.get("schema").cloned()),
    };

    let mut query_record =
        embucket_history::QueryRecord::query_start(&payload.query, payload.worksheet_id);
    let query_res = state
        .execution_svc
        .query(&session_id, &payload.query, query_context)
        .await;

    match query_res {
        Ok((ref records, ref columns)) => {
            let result_set = ResultSet::query_result_to_result_set(records, columns);
            match result_set {
                Ok(result_set) => {
                    let encoded_res = serde_json::to_string(&result_set);

                    if let Ok(encoded_res) = encoded_res {
                        let result_count = i64::try_from(records.len()).unwrap_or(0);
                        query_record.query_finished(result_count, Some(encoded_res));
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
    }

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
    path = "/ui/queries",
    operation_id = "getQueries",
    tags = ["queries"],
    params(
        ("worksheetId" = Option<WorksheetId>, Query, description = "Worksheet id"),
        ("sqlText" = Option<String>, Query, description = "Sql text filter"),
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
    let cursor = params.cursor;
    let result = state.history.get_queries(params.into()).await;

    match result {
        Ok(recs) => {
            let next_cursor = if let Some(last_item) = recs.last() {
                last_item.next_cursor()
            } else {
                embucket_history::QueryRecord::min_cursor() // no items in range -> go to beginning
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
                current_cursor: cursor,
                next_cursor,
            }))
        }
        Err(e) => Err(QueriesAPIError::Queries {
            source: QueryError::Store { source: e },
        }),
    }
}
