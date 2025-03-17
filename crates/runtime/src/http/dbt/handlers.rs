// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::error::{self as dbt_error, DbtError, DbtResult};
use crate::execution::query::IceBucketQueryContext;
use crate::execution::utils::{
    records_to_arrow_string, records_to_json_string, DataSerializationFormat,
};
use crate::http::dbt::schemas::{
    JsonResponse, LoginData, LoginRequestBody, LoginRequestQuery, LoginResponse, QueryRequest,
    QueryRequestBody, ResponseData,
};
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use flate2::read::GzDecoder;
use regex::Regex;
use snafu::ResultExt;
use std::io::Read;
use tracing::debug;
use uuid::Uuid;

#[tracing::instrument(level = "debug", skip(state, body), err, ret(level = tracing::Level::TRACE))]
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
        .metastore
        .list_databases()
        .await
        .map_err(|e| DbtError::Metastore { source: e.into() })?;

    debug!("login request query: {query:?}, databases: {warehouses:?}");
    for warehouse in warehouses
        .into_iter()
        .filter(|w| w.ident == query.database_name)
    {
        // Save warehouse id and db name in state
        state.dbt_sessions.lock().await.insert(
            token.clone(),
            format!("{}.{}", warehouse.ident, query.database_name),
        );
    }
    Ok(Json(LoginResponse {
        data: Option::from(LoginData { token }),
        success: true,
        message: Option::from("successfully executed".to_string()),
    }))
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

    let Some(token) = extract_token(&headers) else {
        return Err(DbtError::MissingAuthToken);
    };

    let sessions = state.dbt_sessions.lock().await;
    let Some(_auth_data) = sessions.get(token.as_str()) else {
        return Err(DbtError::MissingDbtSession);
    };

    // let _ = log_query(&body_json.sql_text).await;

    let (records, columns) = state
        .execution_svc
        .query(
            &session_id,
            &body_json.sql_text,
            IceBucketQueryContext::default(),
        )
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
