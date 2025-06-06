use crate::queries::error::{GetQueryRecordSnafu, StoreSnafu};
use crate::queries::models::{
    GetQueriesParams, QueriesResponse, QueryCreatePayload, QueryCreateResponse, QueryGetResponse,
    QueryRecord,
};
use crate::state::AppState;
use crate::{
    error::ErrorResponse,
    queries::error::{QueriesAPIError, QueriesResult, QueryError, QuerySnafu},
};
use api_sessions::DFSessionId;
use axum::extract::ConnectInfo;
use axum::extract::Path;
use axum::{
    Json,
    extract::{Query, State},
};
use core_executor::models::{QueryContext, QueryResult};
use core_history::{QueryRecordId, WorksheetId};
use core_utils::iterable::IterableEntity;
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(query, queries, get_query),
    components(schemas(QueriesResponse, QueryCreateResponse, QueryCreatePayload, QueryGetResponse, QueryRecord, QueryRecordId, ErrorResponse)),
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
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 409, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::query", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(payload): Json<QueryCreatePayload>,
) -> QueriesResult<Json<QueryCreateResponse>> {
    //
    // Note: This handler allowed to return error from a designated place only,
    // after query record successfuly saved result or error.

    let query_context = QueryContext::new(
        payload
            .context
            .as_ref()
            .and_then(|c| c.get("database").cloned()),
        payload
            .context
            .as_ref()
            .and_then(|c| c.get("schema").cloned()),
        payload.worksheet_id,
    )
    .with_ip_address(addr.ip().to_string());

    let query_res = state
        .execution_svc
        .query(&session_id, &payload.query, query_context)
        .await;

    match query_res {
        Ok(QueryResult { query_id, .. }) => match state.history_store.get_query(query_id).await {
            Err(err) => Err(QueriesAPIError::Query {
                source: QueryError::Store { source: err },
            }),
            Ok(query_record) => Ok(Json(QueryCreateResponse(
                QueryRecord::try_from(query_record).context(QuerySnafu)?,
            ))),
        },
        Err(err) => Err(QueriesAPIError::Query {
            source: QueryError::Execution { source: err },
        }),
    }
}

#[utoipa::path(
    get,
    path = "/ui/queries/{queryRecordId}",
    operation_id = "getQuery",
    tags = ["queries"],
    params(
        ("queryRecordId" = QueryRecordId, Path, description = "Query Record Id")
    ),
    responses(
        (status = 200, description = "Returns result of the query", body = QueryGetResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad query record id", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::get_query", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_query(
    State(state): State<AppState>,
    Path(query_record_id): Path<QueryRecordId>,
) -> QueriesResult<Json<QueryGetResponse>> {
    state
        .history_store
        .get_query(query_record_id)
        .await
        .map(|query_record| {
            Ok(Json(QueryGetResponse(
                query_record.try_into().context(GetQueryRecordSnafu)?,
            )))
        })
        .context(StoreSnafu)
        .context(GetQueryRecordSnafu)?
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
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad worksheet key", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::queries", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn queries(
    Query(params): Query<GetQueriesParams>,
    State(state): State<AppState>,
) -> QueriesResult<Json<QueriesResponse>> {
    let cursor = params.cursor;
    let result = state.history_store.get_queries(params.into()).await;

    match result {
        Ok(recs) => {
            let next_cursor = if let Some(last_item) = recs.last() {
                last_item.next_cursor()
            } else {
                core_history::QueryRecord::min_cursor() // no items in range -> go to beginning
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
