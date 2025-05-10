use crate::error::{self as dbt_error, DbtError, DbtResult};
use crate::schemas::{
    JsonResponse, LoginData, LoginRequestBody, LoginRequestQuery, LoginResponse, QueryRequest,
    QueryRequestBody, ResponseData,
};
use crate::state::AppState;
use api_sessions::DFSessionId;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use base64;
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::prelude::*;
use core_executor::{query::QueryContext, utils::DataSerializationFormat};
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::record_batch::RecordBatch;
use flate2::read::GzDecoder;
use regex::Regex;
use snafu::ResultExt;
use std::io::Read;
use tracing::debug;
use uuid::Uuid;

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
// Buffer Alignment and Padding: Implementations are recommended to allocate memory
// on aligned addresses (multiple of 8- or 64-bytes) and pad (overallocate) to a
// length that is a multiple of 8 or 64 bytes. When serializing Arrow data for interprocess
// communication, these alignment and padding requirements are enforced.
// For more info see issue #115
const ARROW_IPC_ALIGNMENT: usize = 8;

#[tracing::instrument(level = "debug", skip(_state, body), err, ret(level = tracing::Level::TRACE))]
pub async fn login(
    State(_state): State<AppState>,
    Query(query): Query<LoginRequestQuery>,
    body: Bytes,
) -> DbtResult<Json<LoginResponse>> {
    // Decompress the gzip-encoded body
    // TODO: Investigate replacing this with a middleware
    let mut d = GzDecoder::new(&body[..]);
    let mut s = String::new();
    d.read_to_string(&mut s)
        .context(dbt_error::GZipDecompressSnafu)?;

    // Deserialize the JSON body
    let _body_json: LoginRequestBody =
        serde_json::from_str(&s).context(dbt_error::LoginRequestParseSnafu)?;

    let token = Uuid::new_v4().to_string();

    Ok(Json(LoginResponse {
        data: Option::from(LoginData { token }),
        success: true,
        message: Option::from("successfully executed".to_string()),
    }))
}

fn records_to_arrow_string(recs: &Vec<RecordBatch>) -> Result<String, DbtError> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::try_new(ARROW_IPC_ALIGNMENT, false, MetadataVersion::V5)
        .context(dbt_error::ArrowSnafu)?;
    if !recs.is_empty() {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, recs[0].schema_ref(), options)
                .context(dbt_error::ArrowSnafu)?;
        for rec in recs {
            writer.write(rec).context(dbt_error::ArrowSnafu)?;
        }
        writer.finish().context(dbt_error::ArrowSnafu)?;
        drop(writer);
    }
    Ok(engine_base64.encode(buf))
}

fn records_to_json_string(recs: &[RecordBatch]) -> Result<String, DbtError> {
    let buf = Vec::new();
    let write_builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = write_builder.build::<_, JsonArray>(buf);
    let record_refs: Vec<&RecordBatch> = recs.iter().collect();
    writer
        .write_batches(&record_refs)
        .context(dbt_error::ArrowSnafu)?;
    writer.finish().context(dbt_error::ArrowSnafu)?;

    // Get the underlying buffer back,
    String::from_utf8(writer.into_inner()).context(dbt_error::Utf8Snafu)
}

#[tracing::instrument(level = "debug", skip(state, body), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Query(query): Query<QueryRequest>,
    headers: HeaderMap,
    body: Bytes,
) -> DbtResult<Json<JsonResponse>> {
    // Decompress the gzip-encoded body
    let mut d = GzDecoder::new(&body[..]);
    let mut s = String::new();
    d.read_to_string(&mut s)
        .context(dbt_error::GZipDecompressSnafu)?;

    // Deserialize the JSON body
    let body_json: QueryRequestBody =
        serde_json::from_str(&s).context(dbt_error::QueryBodyParseSnafu)?;

    let Some(_token) = extract_token(&headers) else {
        return Err(DbtError::MissingAuthToken);
    };

    let (records, columns) = state
        .execution_svc
        .query(&session_id, &body_json.sql_text, QueryContext::default())
        .await
        .map_err(|e| DbtError::Execution { source: e })?;

    debug!(
        "serialized json: {}",
        records_to_json_string(&records)?.as_str()
    );

    let serialization_format = state.execution_svc.config().dbt_serialization_format;
    let json_resp = Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: columns.into_iter().map(Into::into).collect(),
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
            error_code: None,
            sql_state: Option::from("ok".to_string()),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: Some(format!("{:06}", 200)),
    });
    debug!(
        "query {:?}, response: {:?}, records: {:?}",
        body_json.sql_text, json_resp, records
    );
    Ok(json_resp)
}

pub async fn abort() -> DbtResult<Json<serde_json::value::Value>> {
    Err(DbtError::NotImplemented)
}

#[must_use]
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers.get("authorization").and_then(|value| {
        value.to_str().ok().and_then(|auth| {
            #[allow(clippy::unwrap_used)]
            let re = Regex::new(r#"Snowflake Token="([a-f0-9\-]+)""#).unwrap();
            re.captures(auth)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        })
    })
}
