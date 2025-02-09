use super::error::{self as dbt_error, DbtError, DbtResult};
use crate::http::dbt::schemas::{
    JsonResponse, LoginData, LoginRequestBody, LoginRequestQuery, LoginResponse, QueryRequest,
    QueryRequestBody, ResponseData,
};
use crate::http::session::DFSessionId;
use crate::state::AppState;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::MetadataVersion;
use arrow::json::writer::JsonArray;
use arrow::json::WriterBuilder;
use arrow::record_batch::RecordBatch;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use base64;
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::prelude::*;
use flate2::read::GzDecoder;
use regex::Regex;
use snafu::ResultExt;
use std::io::Read;
use tracing::debug;
use uuid::Uuid;

const SERIALIZATION_FORMAT: &str = "arrow"; // or "json"
const ARROW_IPC_ALIGNMENT: usize = 8;

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn login(
    State(state): State<AppState>,
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

    let warehouses = state
        .control_svc
        .list_warehouses()
        .await
        .context(dbt_error::ControlServiceSnafu)?;

    for warehouse in warehouses.into_iter().filter(|w| w.name == query.warehouse) {
        // Save warehouse id and db name in state
        state.dbt_sessions.lock().await.insert(
            token.clone(),
            format!("{}.{}", warehouse.id, query.database_name),
        );
    }
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
    };
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

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
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

    let Some(token) = extract_token(&headers) else {
        return Err(DbtError::MissingAuthToken);
    };

    let sessions = state.dbt_sessions.lock().await;

    let Some(_auth_data) = sessions.get(token.as_str()) else {
        return Err(DbtError::MissingDbtSession);
    };

    // let _ = log_query(&body_json.sql_text).await;

    let (records, columns) = state
        .control_svc
        .query(&session_id, &body_json.sql_text)

        .await
        .context(dbt_error::ControlServiceSnafu)?;

    debug!("serialized json: {}", records_to_json_string(&records)?.as_str());

    let json_resp = Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: columns.into_iter().map(Into::into).collect(),
            query_result_format: Option::from(String::from(SERIALIZATION_FORMAT)),
            row_set: if SERIALIZATION_FORMAT == "json" {
                Option::from(ResponseData::rows_to_vec(
                    records_to_json_string(&records)?.as_str(),
                )?)
            } else {
                None
            },
            row_set_base_64: if SERIALIZATION_FORMAT == "arrow" {
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

/*async fn log_query(query: &str) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("queries.log")
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    file.write_all(query.as_bytes()).await?;
    file.write_all(b"\n").await?;
    Ok(())
}*/

// mod tests {
//     use std::io::Cursor;
//     use arrow::record_batch::RecordBatch;
//     use arrow::ipc::writer::{ FileWriter, StreamWriter, IpcWriteOptions };
//     use arrow::ipc::reader::{ FileReader, StreamReader };
//     use arrow::array::{ArrayRef, StringArray, UInt32Array};
//     use arrow::datatypes::{ Schema, Field, DataType };
//     use tracing::span::Record;
//     use std::sync::Arc;
//     use std::any::Any;

//     // fn roundtrip_ipc_stream(rb: &RecordBatch) -> RecordBatch {
//     //     let mut buf = Vec::new();
//     //     let mut writer = StreamWriter::try_new(&mut buf, rb.schema_ref()).unwrap();
//     //     writer.write(rb).unwrap();
//     //     writer.finish().unwrap();
//     //     drop(writer);

//     //     let mut reader = StreamReader::try_new(std::io::Cursor::new(buf), None).unwrap();
//     //     reader.next().unwrap().unwrap()
//     // }

//     // if records.len() > 0 {
//     //     println!("agahaha {:?}", roundtrip_ipc_stream(&records[0]));
//     // }

//     // Try to add flatbuffer verification
//     ///////////////////
//     // let base64 = general_purpose::STANDARD.encode(buffer);
//     // Ok((base64, columns))
//     // let encoded = general_purpose::STANDARD.decode(res.clone()).unwrap();

//     // let mut verifier = Verifier::new(&VerifierOptions::default(), &encoded);
//     // let mut builder = FlatBufferBuilder::new();
//     // let result = general_purpose::STANDARD.encode(buf);
//     ///////////////////

//     #[test]
//     fn test_slice_uint32() {
//    /// Read/write a record batch to a File and Stream and ensure it is the same at the outout
//         fn ensure_roundtrip(array: ArrayRef) {
//             let num_rows = array.len();
//             let orig_batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
//             // take off the first element
//             let sliced_batch = orig_batch.slice(1, num_rows - 1);

//             let schema = orig_batch.schema();
//             let stream_data = {
//                 let mut writer = StreamWriter::try_new(vec![], &schema).unwrap();
//                 writer.write(&sliced_batch).unwrap();
//                 writer.into_inner().unwrap()
//             };
//             let read_batch = {
//                 let projection = None;
//                 let mut reader = StreamReader::try_new(Cursor::new(stream_data), projection).unwrap();
//                 reader
//                     .next()
//                     .expect("expect no errors reading batch")
//                     .expect("expect batch")
//             };
//             assert_eq!(sliced_batch, read_batch);

//             let file_data = {
//                 let mut writer = FileWriter::try_new_buffered(vec![], &schema).unwrap();
//                 writer.write(&sliced_batch).unwrap();
//                 writer.into_inner().unwrap().into_inner().unwrap()
//             };
//             let read_batch = {
//                 let projection = None;
//                 let mut reader = FileReader::try_new(Cursor::new(file_data), projection).unwrap();
//                 reader
//                     .next()
//                     .expect("expect no errors reading batch")
//                     .expect("expect batch")
//             };
//             assert_eq!(sliced_batch, read_batch);

//             // TODO test file writer/reader
//         }

//         ensure_roundtrip(Arc::new(UInt32Array::from_iter((0..8).map(|i| {
//             if i % 2 == 0 {
//                 Some(i)
//             } else {
//                 None
//             }
//         }))));
//     }

//     #[test]
//     fn test_flatbuffer_issue_8150() {
//         fn roundtrip_ipc_stream(rb: &RecordBatch) -> RecordBatch {
//             let mut buf = Vec::new();
//             let mut writer = StreamWriter::try_new(&mut buf, rb.schema_ref()).unwrap();
//             writer.write(rb).unwrap();
//             writer.finish().unwrap();
//             drop(writer);

//             let mut reader = StreamReader::try_new(std::io::Cursor::new(buf), None).unwrap();
//             reader.next().unwrap().unwrap()
//         }

//         let schema = Schema::new(vec![
//             Field::new("hours", DataType::UInt32, true),
//             Field::new("minutes", DataType::UInt32, true),
//         ]);
//         let batch = RecordBatch::try_new(
//         Arc::new(schema),
//         vec![
//                 Arc::new(UInt32Array::from(vec![4, 7, 12, 16])),
//                 Arc::new(UInt32Array::from(vec![20, 40, 00, 20])),
//             ]
//         ).unwrap();
//         roundtrip_ipc_stream(&batch);
//     }
// }
