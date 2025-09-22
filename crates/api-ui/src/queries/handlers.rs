#![allow(clippy::needless_for_each)]
use crate::queries::error::{GetQueryRecordSnafu, QueryRecordResult, StoreSnafu};
use crate::queries::models::{
    GetQueriesParams, QueriesResponse, QueryCreatePayload, QueryCreateResponse, QueryGetResponse,
    QueryRecord, QueryRecordId,
};
use crate::state::AppState;
use crate::{apply_parameters, downcast_int64_column, downcast_string_column, error::ErrorResponse, error::Result, queries::error::{self as queries_errors, QueryError}, SearchParameters};
use api_sessions::DFSessionId;
use axum::extract::ConnectInfo;
use axum::extract::Path;
use axum::{
    Json,
    extract::{Query, State},
};
use core_executor::models::{QueryContext, QueryResult};
use core_history::WorksheetId;
use core_utils::iterable::IterableEntity;
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use chrono::{DateTime, Utc};
use utoipa::OpenApi;
use crate::worksheets::error::ListSnafu;
use crate::worksheets::Worksheet;

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
#[tracing::instrument(name = "api_ui::query", level = "info", skip(state), fields(query_id) err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(payload): Json<QueryCreatePayload>,
) -> Result<Json<QueryCreateResponse>> {
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
        match payload.worksheet_id {
            None => None,
            Some(worksheet_id) => match state.history_store.get_worksheet(worksheet_id).await {
                Err(_) => None,
                Ok(_) => Some(worksheet_id),
            },
        },
    )
    .with_ip_address(addr.ip().to_string());

    let query_res = state
        .execution_svc
        .query(&session_id, &payload.query, query_context)
        .await;

    match query_res
        .context(queries_errors::ExecutionSnafu)
        .context(queries_errors::QuerySnafu)
    {
        Ok(QueryResult { query_id, .. }) => {
            // Record the result as part of the current span.
            tracing::Span::current().record("query_id", query_id.as_i64());
            let query_record = state
                .history_store
                .get_query(query_id)
                .await
                .map(QueryRecord::try_from)
                .context(queries_errors::StoreSnafu)
                .context(queries_errors::QuerySnafu)?
                .context(queries_errors::QuerySnafu)?;
            return Ok(Json(QueryCreateResponse(query_record)));
        }
        Err(err) => Err(err.into()), // convert queries Error into crate Error
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
) -> Result<Json<QueryGetResponse>> {
    state
        .history_store
        .get_query(query_record_id.into())
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
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
) -> Result<Json<QueriesResponse>> {
    let context = QueryContext::default();
    let sql_string = "SELECT * FROM slatedb.history.queries".to_string();
    let sql_string = apply_parameters(&sql_string, parameters, &["id", "worksheet_id", "query", "status"]);
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ListSnafu)?;
    let mut items = Vec::new();
    for record in records {
        let ids = downcast_int64_column(&record, "id").context(ListSnafu)?;
        let names = downcast_string_column(&record, "name").context(ListSnafu)?;
        let contents = downcast_string_column(&record, "content").context(ListSnafu)?;
        let created_at_timestamps =
            downcast_string_column(&record, "created_at").context(ListSnafu)?;
        let updated_at_timestamps =
            downcast_string_column(&record, "updated_at").context(ListSnafu)?;
        for i in 0..record.num_rows() {
            items.push(Worksheet {
                id: ids.value(i),
                name: names.value(i).to_string(),
                content: contents.value(i).to_string(),
                created_at: created_at_timestamps
                    .value(i)
                    .parse::<DateTime<Utc>>()
                    .context(DatetimeSnafu)?,
                updated_at: updated_at_timestamps
                    .value(i)
                    .parse::<DateTime<Utc>>()
                    .context(DatetimeSnafu)?,
            });
        }
    }
    Ok(Json(QueriesResponse {
        items,
    }))
}
