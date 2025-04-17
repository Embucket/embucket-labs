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

use super::error::{
    CreateResultSetSnafu, QueryError, QueryRecordResult, ResultParseSnafu, Utf8Snafu,
};
use crate::execution::models::ColumnInfo;
use arrow::array::RecordBatch;
use arrow_json::{writer::JsonArray, WriterBuilder};
use chrono::{DateTime, Utc};
use embucket_history::{QueryRecord as QueryRecordItem, QueryRecordId, QueryStatus, WorksheetId};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::collections::HashMap;
use utoipa::ToSchema;

pub type ExecutionContext = crate::execution::query::QueryContext;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = Row, value_type = Vec<Value>)]
pub struct Row(Vec<Value>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl ResultSet {
    pub fn query_result_to_result_set(
        records: &[RecordBatch],
        columns: &[ColumnInfo],
    ) -> std::result::Result<Self, QueryError> {
        let buf = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(buf);

        // serialize records to str
        let records: Vec<&RecordBatch> = records.iter().collect();
        writer
            .write_batches(&records)
            .context(CreateResultSetSnafu)?;
        writer.finish().context(CreateResultSetSnafu)?;

        // Get the underlying buffer back,
        let buf = writer.into_inner();
        let record_batch_str = String::from_utf8(buf).context(Utf8Snafu)?;

        // convert to array, leaving only values
        let rows: Vec<IndexMap<String, Value>> =
            serde_json::from_str(record_batch_str.as_str()).context(ResultParseSnafu)?;
        let rows: Vec<Row> = rows
            .into_iter()
            .map(|obj| Row(obj.values().cloned().collect()))
            .collect();

        let columns = columns
            .iter()
            .map(|ci| Column {
                name: ci.name.clone(),
                r#type: ci.r#type.clone(),
            })
            .collect();

        Ok(Self { columns, rows })
    }
}

impl TryFrom<&str> for ResultSet {
    type Error = QueryError;

    fn try_from(result: &str) -> QueryRecordResult<Self> {
        serde_json::from_str(result).context(ResultParseSnafu)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreatePayload {
    pub worksheet_id: Option<WorksheetId>,
    pub query: String,
    pub context: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryCreateResponse {
    #[serde(flatten)]
    pub data: QueryRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryRecord {
    pub id: QueryRecordId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worksheet_id: Option<WorksheetId>,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub result: ResultSet,
    pub status: QueryStatus,
    pub error: String, // empty error - ok
}

impl TryFrom<QueryRecordItem> for QueryRecord {
    type Error = QueryError;

    fn try_from(query: QueryRecordItem) -> QueryRecordResult<Self> {
        let query_result = query.result.unwrap_or_default();
        let query_error = query.error.unwrap_or_default();
        let result_set = if query_result.is_empty() {
            ResultSet {
                rows: vec![],
                columns: vec![],
            }
        } else {
            ResultSet::try_from(query_result.as_str())?
        };
        Ok(Self {
            id: query.id,
            worksheet_id: query.worksheet_id,
            query: query.query,
            start_time: query.start_time,
            end_time: query.end_time,
            duration_ms: query.duration_ms,
            result_count: query.result_count,
            status: query.status,
            result: result_set,
            error: query_error,
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
#[serde(rename_all = "camelCase")]
pub struct GetQueriesParams {
    pub worksheet_id: Option<WorksheetId>,
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}
