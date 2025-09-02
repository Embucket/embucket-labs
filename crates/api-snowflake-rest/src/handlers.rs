use crate::error::{self as api_snowflake_rest_error, Error, Result};
use crate::models::{
    JsonResponse, LoginRequestBody, LoginRequestData, LoginResponse, LoginResponseData,
    QueryRequest, QueryRequestBody, ResponseData,
};
use crate::state::AppState;
use api_sessions::DFSessionId;
use axum::Json;
use axum::extract::{ConnectInfo, Query, State};
use base64;
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::prelude::*;
use core_executor::models::QueryContext;
use core_executor::utils::{DataSerializationFormat, convert_record_batches};
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use std::net::SocketAddr;
use tracing::debug;
use uuid::Uuid;

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
// Buffer Alignment and Padding: Implementations are recommended to allocate memory
// on aligned addresses (multiple of 8- or 64-bytes) and pad (overallocate) to a
// length that is a multiple of 8 or 64 bytes. When serializing Arrow data for interprocess
// communication, these alignment and padding requirements are enforced.
// For more info see issue #115
const ARROW_IPC_ALIGNMENT: usize = 8;

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

fn records_to_arrow_string(recs: &Vec<RecordBatch>) -> std::result::Result<String, Error> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::try_new(ARROW_IPC_ALIGNMENT, false, MetadataVersion::V5)
        .context(api_snowflake_rest_error::ArrowSnafu)?;
    if !recs.is_empty() {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, recs[0].schema_ref(), options)
                .context(api_snowflake_rest_error::ArrowSnafu)?;
        for rec in recs {
            writer
                .write(rec)
                .context(api_snowflake_rest_error::ArrowSnafu)?;
        }
        writer
            .finish()
            .context(api_snowflake_rest_error::ArrowSnafu)?;
        drop(writer);
    }
    Ok(engine_base64.encode(buf))
}

fn records_to_json_string(recs: &[RecordBatch]) -> std::result::Result<String, Error> {
    let buf = Vec::new();
    let write_builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = write_builder.build::<_, JsonArray>(buf);
    let record_refs: Vec<&RecordBatch> = recs.iter().collect();
    writer
        .write_batches(&record_refs)
        .context(api_snowflake_rest_error::ArrowSnafu)?;
    writer
        .finish()
        .context(api_snowflake_rest_error::ArrowSnafu)?;

    // Get the underlying buffer back,
    String::from_utf8(writer.into_inner()).context(api_snowflake_rest_error::Utf8Snafu)
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
    let query_context = QueryContext::default().with_ip_address(addr.ip().to_string());

    if async_exec {
        let query_handle = state
            .execution_svc
            .submit_query(&session_id, &sql_text, query_context)
            .await?;
        let query_uuid: Uuid = query_handle.query_id.to_uuid();
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
            code: Some(format!("{:06}", 200)),
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
    // No need to fetch underlying error for snafu(transparent)
    let records = convert_record_batches(query_result.clone(), serialization_format)?;
    debug!(
        "serialized json: {}",
        records_to_json_string(&records)?.as_str()
    );
    let query_uuid: Uuid = query_result.query_id.to_uuid();
    // Record the result as part of the current span.
    tracing::Span::current()
        .record("query_id", query_result.query_id.as_i64())
        .record("query_uuid", query_uuid.to_string());

    let json_resp = Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: query_result
                .column_info()
                .into_iter()
                .map(Into::into)
                .collect(),
            query_result_format: Some(serialization_format.to_string().to_lowercase()),
            row_set: if serialization_format == DataSerializationFormat::Json {
                Option::from(ResponseData::rows_to_vec(
                    records_to_json_string(&records)?.as_str(),
                )?)
            } else {
                None
            },
            row_set_base_64: if serialization_format == DataSerializationFormat::Arrow {
                Option::from(records_to_arrow_string(&records)?)
            } else {
                None
            },
            total: Some(1),
            query_id: Some(query_uuid.to_string()),
            error_code: None,
            sql_state: Option::from("ok".to_string()),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: Some(format!("{:06}", 200)),
    });
    debug!(
        "query {:?}, response: {:?}, records: {:?}",
        sql_text, json_resp, records
    );
    Ok(json_resp)
}

pub async fn abort() -> Result<Json<serde_json::value::Value>> {
    api_snowflake_rest_error::NotImplementedSnafu.fail()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
mod tests {
    use crate::models::{
        ClientEnvironment, JsonResponse, LoginRequestBody, LoginRequestData, LoginResponse,
        QueryRequestBody,
    };
    use crate::test_server::run_test_server_with_demo_auth;
    use axum::body::Bytes;
    use axum::http;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use reqwest::Method;
    use reqwest::header::AUTHORIZATION;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::io::Write;

    #[tokio::test]
    async fn test_login() {
        let addr =
            run_test_server_with_demo_auth("embucket".to_string(), "embucket".to_string()).await;
        let client = reqwest::Client::new();
        let login_url = format!("http://{addr}/session/v1/login-request");
        let query_url = format!("http://{addr}/queries/v1/query-request");

        let query_request = QueryRequestBody {
            sql_text: "SELECT 1;".to_string(),
            async_exec: false,
        };

        let query_compressed_bytes = make_bytes_body(&query_request);

        assert!(
            !query_compressed_bytes.is_empty(),
            "Compressed data should not be empty"
        );

        //Check before login without an auth header
        let res = client
            .request(Method::POST, format!("{query_url}?requestId=123"))
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::UNAUTHORIZED, res.status());

        let login_request = LoginRequestBody {
            data: LoginRequestData {
                client_app_id: String::new(),
                client_app_version: String::new(),
                svn_revision: None,
                account_name: String::new(),
                login_name: "embucket".to_string(),
                client_environment: ClientEnvironment {
                    application: String::new(),
                    os: String::new(),
                    os_version: String::new(),
                    python_version: String::new(),
                    python_runtime: String::new(),
                    python_compiler: String::new(),
                    ocsp_mode: String::new(),
                    tracing: 0,
                    login_timeout: None,
                    network_timeout: None,
                    socket_timeout: None,
                },
                password: "embucket".to_string(),
                session_parameters: HashMap::default(),
            },
        };

        let login_compressed_bytes = make_bytes_body(&login_request);

        assert!(
            !login_compressed_bytes.is_empty(),
            "Compressed data should not be empty"
        );

        //Login
        let res = client
            .request(
                Method::POST,
                format!(
                    "{login_url}?request_id=123&databaseName=embucket&schemaName=public&warehouse=embucket"
                ),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(login_compressed_bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::OK, res.status());
        let login_response: LoginResponse = res.json().await.unwrap();
        assert!(login_response.data.is_some());
        assert!(login_response.success);
        assert!(login_response.message.is_some());

        //Check after login without an auth header
        let res = client
            .request(Method::POST, format!("{query_url}?requestId=123"))
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::UNAUTHORIZED, res.status());

        //Check after login with an auth header
        let res = client
            .request(Method::POST, format!("{query_url}?requestId=123"))
            .header(
                AUTHORIZATION,
                format!("Snowflake Token=\"{}\"", login_response.data.unwrap().token),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::OK, res.status());
        let query_response: JsonResponse = res.json().await.unwrap();
        assert!(query_response.data.is_some());
        assert!(query_response.success);
        assert!(query_response.message.is_some());
        assert!(query_response.code.is_some());
    }
    fn make_bytes_body<T: ?Sized + Serialize>(request: &T) -> Bytes {
        let json = serde_json::to_string(request).expect("Failed to serialize JSON");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(json.as_bytes())
            .expect("Failed to write to encoder");
        let compressed_data = encoder.finish().expect("Failed to finish compression");

        Bytes::from(compressed_data)
    }
}
