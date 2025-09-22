use super::state::AppState;
use crate::models::{
    AbortRequestBody, JsonResponse, LoginRequestBody, LoginRequestData, LoginResponse,
    LoginResponseData, QueryRequest, QueryRequestBody, ResponseData,
};
use crate::server::error::{
    self as api_snowflake_rest_error, Error, InvalidUuidFormatSnafu, Result,
};
use crate::server::helpers::prepare_query_ok_response;
use api_sessions::DFSessionId;
use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State};
use core_executor::AbortQuery;
use core_executor::error as ex_error;
use core_executor::models::QueryContext;
use core_history::{QueryIdParam, QueryRecordId};
use snafu::{ResultExt, location};
use std::net::SocketAddr;
use std::str::FromStr;
use uuid::Uuid;

#[tracing::instrument(name = "api_snowflake_rest::login", level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn login(
    State(state): State<AppState>,
    // Query(_query_params): Query<LoginRequestQueryParams>,
    Json(LoginRequestBody {
        data:
            LoginRequestData {
                login_name,
                password,
                ..
            },
    }): Json<LoginRequestBody>,
) -> Result<Json<LoginResponse>> {
    if login_name != *state.config.auth.demo_user || password != *state.config.auth.demo_password {
        return api_snowflake_rest_error::InvalidAuthDataSnafu.fail()?;
    }

    let session_id = uuid::Uuid::new_v4().to_string();

    let _ = state.execution_svc.create_session(&session_id).await?;

    Ok(Json(LoginResponse {
        data: Option::from(LoginResponseData { token: session_id }),
        success: true,
        message: Option::from("successfully executed".to_string()),
    }))
}

#[tracing::instrument(name = "api_snowflake_rest::query", level = "debug", skip(state), fields(query_id, query_uuid), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Query(query): Query<QueryRequest>,
    Json(QueryRequestBody {
        sql_text,
        async_exec,
    }): Json<QueryRequestBody>,
) -> Result<Json<JsonResponse>> {
    let serialization_format = state.config.dbt_serialization_format;
    let query_context = QueryContext::default()
        .with_ip_address(addr.ip().to_string())
        .with_async_query(async_exec)
        .with_request_id(Uuid::from_str(&query.request_id).context(InvalidUuidFormatSnafu)?);

    if async_exec {
        let query_handle = state
            .execution_svc
            .submit_query(&session_id, &sql_text, query_context)
            .await?;
        let query_uuid: Uuid = query_handle.query_id.as_uuid();
        // Record the result as part of the current span.
        tracing::Span::current()
            .record("query_id", query_handle.query_id.as_i64())
            .record("query_uuid", query_uuid.to_string());

        return Ok(Json(JsonResponse {
            data: Option::from(ResponseData {
                query_id: Some(query_uuid.to_string()),
                ..Default::default()
            }),
            success: true,
            message: Option::from("successfully executed".to_string()),
            code: None,
        }));
    }

    let query_result = state
        .execution_svc
        .query(
            &session_id,
            &sql_text,
            QueryContext::default().with_ip_address(addr.ip().to_string()),
        )
        .await?;

    prepare_query_ok_response(&sql_text, query_result, serialization_format)
}

#[tracing::instrument(name = "api_snowflake_rest::get_query", level = "debug", skip(state), fields(query_id, query_uuid), err, ret(level = tracing::Level::TRACE))]
pub async fn get_query(
    State(state): State<AppState>,
    Path(query_id): Path<QueryIdParam>,
) -> Result<Json<JsonResponse>> {
    let query_id: QueryRecordId = query_id.into();

    let query_uuid: Uuid = query_id.as_uuid();
    // Record the result as part of the current span.
    tracing::Span::current()
        .record("query_id", query_id.as_i64())
        .record("query_uuid", query_uuid.to_string());

    let query_result = state
        .execution_svc
        .wait_historical_query_result(query_id)
        .await?;
    match query_result {
        Ok(query_result) => {
            prepare_query_ok_response("", query_result, state.config.dbt_serialization_format)
        }
        // Return the same response as it would be returned when error is propagated
        Err(error) => {
            // Create without using build(), and not using context which works with result
            let error = Error::Execution {
                source: ex_error::Error::QueryExecution {
                    query_id,
                    source: Box::new(error),
                    location: location!(),
                },
            };

            let (_http_code, body) = error.prepare_response();
            Ok(body)
        }
    }
}

#[tracing::instrument(name = "api_snowflake_rest::abort", level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn abort(
    State(state): State<AppState>,
    Query(QueryRequest { request_id }): Query<QueryRequest>,
    Json(AbortRequestBody { sql_text, .. }): Json<AbortRequestBody>,
) -> Result<Json<serde_json::value::Value>> {
    let request_id = Uuid::from_str(&request_id).context(InvalidUuidFormatSnafu)?;
    state
        .execution_svc
        .abort_query(AbortQuery::ByRequestId(request_id, sql_text))?;
    Ok(Json(serde_json::value::Value::Null))
}
