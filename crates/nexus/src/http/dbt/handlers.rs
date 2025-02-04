use super::error::{self as dbt_error, DbtError, DbtResult};
use crate::http::dbt::schemas::{
    JsonResponse, LoginData, LoginRequestBody, LoginRequestQuery, LoginResponse, QueryRequest,
    QueryRequestBody, ResponseData,
};
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use flate2::read::GzDecoder;
use regex::Regex;
use snafu::ResultExt;
use std::io::Read;
use uuid::Uuid;

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

    //println!("Received login request: {:?}", query);
    //println!("Body data parameters: {:?}", body_json);
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

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn query(
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

    let (result, columns) = state
        .control_svc
        .query_dbt(&body_json.sql_text)
        .await
        .context(dbt_error::ControlServiceSnafu)?;

    // query_result_format now is JSON since arrow IPC has flatbuffers bug
    // https://github.com/apache/arrow-rs/pull/6426
    Ok(Json(JsonResponse {
        data: Option::from(ResponseData {
            row_type: columns.into_iter().map(Into::into).collect(),
            // row_set_base_64: Option::from(result.clone()),
            row_set_base_64: None,
            #[allow(clippy::unwrap_used)]
            row_set: ResponseData::rows_to_vec(result.as_str())?,
            total: Some(1),
            query_result_format: Option::from("json".to_string()),
            error_code: None,
            sql_state: Option::from("ok".to_string()),
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
        code: Some(format!("{:06}", 200)),
    }))
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
