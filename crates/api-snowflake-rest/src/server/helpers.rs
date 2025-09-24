use crate::SqlState;
use crate::models::{JsonResponse, ResponseData};
use crate::server::error::{self as api_snowflake_rest_error, Error, Result};
use axum::Json;
use base64;
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::prelude::*;
use core_executor::models::QueryResult;
use core_executor::utils::{DataSerializationFormat, convert_record_batches};
use core_history::Row;
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use tracing::debug;
use uuid::Uuid;

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
// Buffer Alignment and Padding: Implementations are recommended to allocate memory
// on aligned addresses (multiple of 8- or 64-bytes) and pad (overallocate) to a
// length that is a multiple of 8 or 64 bytes. When serializing Arrow data for interprocess
// communication, these alignment and padding requirements are enforced.
// For more info see issue #115
const ARROW_IPC_ALIGNMENT: usize = 8;

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

// moved from impl ResponseData, to satisfy features dependencies
impl ResponseData {
    pub fn rows_to_vec(json_rows_string: &str) -> Result<Vec<Row>> {
        serde_json::from_str::<Vec<Row>>(json_rows_string)
            .context(api_snowflake_rest_error::RowParseSnafu)
    }
}

#[tracing::instrument(name = "prepare_query_ok_response", level = "debug", err, ret(level = tracing::Level::TRACE))]
pub fn prepare_query_ok_response(
    sql_text: &str,
    query_result: QueryResult,
    ser_fmt: DataSerializationFormat,
    from_history: bool,
) -> Result<Json<JsonResponse>> {
    let records = if from_history {
        &query_result.records
    } else {
        // only use conversion for non historical data
        &convert_record_batches(&query_result, ser_fmt)?
    };

    let query_uuid: Uuid = query_result.query_id.as_uuid();
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
            query_result_format: Some(ser_fmt.to_string().to_lowercase()),
            row_set: if ser_fmt == DataSerializationFormat::Json {
                Option::from(ResponseData::rows_to_vec(
                    records_to_json_string(records)?.as_str(),
                )?)
            } else {
                None
            },
            row_set_base_64: if ser_fmt == DataSerializationFormat::Arrow {
                Option::from(records_to_arrow_string(records)?)
            } else {
                None
            },
            total: Some(1),
            query_id: Some(query_uuid.to_string()),
            error_code: None,
            sql_state: Some(SqlState::Success.to_string()),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: None,
    });
    debug!(
        "query {:?}, response: {:?}, records: {:?}",
        sql_text, json_resp, records
    );
    Ok(json_resp)
}
