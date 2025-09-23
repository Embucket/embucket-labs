#![allow(clippy::needless_for_each)]
use crate::queries::error::{
    DatetimeSnafu, ExecutionSnafu, GetQueryRecordSnafu, QueriesSnafu, StoreSnafu,
};
use crate::queries::models::{GetQueriesParams, QueriesResponse, QueryCreatePayload, QueryCreateResponse, QueryGetResponse, QueryRecord, QueryRecordId, QueryStatus, ResultSet};
use crate::state::AppState;
use crate::{
    OrderDirection, SearchParameters, apply_parameters, downcast_int64_column,
    downcast_string_column,
    error::ErrorResponse,
    error::Result,
    queries::error::{self as queries_errors},
};
use api_sessions::DFSessionId;
use axum::extract::ConnectInfo;
use axum::extract::Path;
use axum::{
    Json,
    extract::{Query, State},
};
use chrono::DateTime;
use core_executor::models::{QueryContext, QueryResult};
use datafusion::arrow::array::Array;
use snafu::{OptionExt, ResultExt};
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
        ("offset" = Option<usize>, Query, description = "Queries offset"),
        ("limit" = Option<usize>, Query, description = "Queries limit"),
        ("search" = Option<String>, Query, description = "Queries search"),
        ("order_by" = Option<String>, Query, description = "Order by: id (default), worksheet_id, result_count, status, start_time, end_time, duration_ms"),
        ("order_direction" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
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
    Query(special_parameters): Query<GetQueriesParams>,
    State(state): State<AppState>,
) -> Result<Json<QueriesResponse>> {
    let context = QueryContext::default();
    let sql_string = "SELECT * FROM slatedb.history.queries".to_string();
    let sql_string = special_parameters.worksheet_id.map_or_else(
        || sql_string.clone(), 
        |worksheet_id| {
        format!(
            "{sql_string} WHERE worksheet_id = {worksheet_id}"
        )
    });
    let sql_string = special_parameters.min_duration_ms.map_or_else(
        || sql_string.clone(),
    |min_duration_ms| {
        format!(
            "{sql_string} WHERE duration_ms >= {min_duration_ms}"
        )
    });
    let sql_string = apply_parameters(
        &sql_string,
        parameters,
        if special_parameters.worksheet_id.is_some() {
            &["id", "query", "status"]
        } else {
            &["id", "worksheet_id", "query", "status"]
        },
    );
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ExecutionSnafu)
        .context(QueriesSnafu)?;
    let mut items = Vec::new();
    for record in records {
        let ids = downcast_int64_column(&record, "id")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let worksheet_ids = downcast_int64_column(&record, "worksheet_id")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let queries = downcast_string_column(&record, "query")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let start_times = downcast_int64_column(&record, "start_time")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let end_times = downcast_int64_column(&record, "end_time")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let duration_ms_values = downcast_int64_column(&record, "duration_ms")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let result_counts = downcast_int64_column(&record, "result_count")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let results = downcast_string_column(&record, "result")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let status = downcast_string_column(&record, "status")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        let errors = downcast_string_column(&record, "error")
            .context(ExecutionSnafu)
            .context(QueriesSnafu)?;
        for i in 0..record.num_rows() {
            items.push(QueryRecord {
                id: ids.value(i),
                worksheet_id: if worksheet_ids.is_null(i) {
                    None
                } else {
                    Some(worksheet_ids.value(i))
                },
                query: queries.value(i).to_string(),
                start_time: DateTime::from_timestamp_millis(start_times.value(i))
                    .context(DatetimeSnafu)
                    .context(QueriesSnafu)?,
                end_time: DateTime::from_timestamp_millis(end_times.value(i))
                    .context(DatetimeSnafu)
                    .context(QueriesSnafu)?,
                duration_ms: duration_ms_values.value(i),
                result_count: result_counts.value(i),
                result: ResultSet::try_from(results.value(i)).context(QueriesSnafu)?,
                status: QueryStatus::try_from(status.value(i)).context(QueriesSnafu)?,
                error: errors.value(i).to_string(),
            });
        }
    }
    Ok(Json(QueriesResponse { items }))
}
