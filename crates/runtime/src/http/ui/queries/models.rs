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

use super::error::{QueryError, QueryRecordResult, ResultParseSnafu};
use chrono::{DateTime, Utc};
use icebucket_history::{QueryRecord as QueryRecordItem, QueryRecordId, QueryStatus, WorksheetId};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use snafu::ResultExt;
use std::collections::HashMap;
use utoipa::ToSchema;
use validator::Validate;

pub type ExecutionContext = crate::execution::query::IceBucketQueryContext;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreatePayload {
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreateResponse {
    pub id: QueryRecordId,
    pub worksheet_id: WorksheetId,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub result: Vec<Vec<serde_json::Value>>,
    pub status: QueryStatus,
}

impl TryFrom<QueryRecordItem> for QueryCreateResponse {
    type Error = QueryError;

    fn try_from(query: QueryRecordItem) -> QueryRecordResult<Self> {
        match str_to_result(query.result.unwrap_or(String::new()).as_str()) {
            Ok(result) => Ok(Self {
                id: query.id,
                worksheet_id: query.worksheet_id,
                query: query.query,
                start_time: query.start_time,
                end_time: query.end_time,
                duration_ms: query.duration_ms,
                result_count: query.result_count,
                status: query.status,
                result,
            }),
            Err(err) => Err(err),
        }
    }
}

pub(crate) fn str_to_result(result_str: &str) -> QueryRecordResult<Vec<Vec<Value>>> {
    let json_array: Vec<IndexMap<String, Value>> = serde_json::from_str(result_str).context(ResultParseSnafu)?;
    Ok(json_array
        .into_iter()
        .map(|obj| obj.values().cloned().collect())
        .collect())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryRecord {
    #[serde(flatten)]
    pub data: QueryCreateResponse,
    pub error: Option<String>,
}

impl TryFrom<QueryRecordItem> for QueryRecord {
    type Error = QueryError;

    fn try_from(query: QueryRecordItem) -> QueryRecordResult<Self> {
        Ok(Self {
            error: query.error.clone(),
            data: QueryCreateResponse::try_from(query)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueriesResponse {
    pub items: Vec<QueryRecord>,
    pub current_cursor: Option<QueryRecordId>,
    pub next_cursor: QueryRecordId,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetHistoryItemsParams {
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}
