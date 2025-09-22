use super::error::{QueryError, QueryRecordResult, ResultParseSnafu};
use crate::default_limit;
use chrono::{DateTime, Utc};
use core_history::{QueryStatus as QueryStatusItem, WorksheetId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::collections::HashMap;
use utoipa::ToSchema;

pub type ExecutionContext = core_executor::models::QueryContext;

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
pub struct QueryCreateResponse(pub QueryRecord);

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Running,
    Successful,
    Failed,
    Canceled,
    TimedOut,
}

impl From<QueryStatusItem> for QueryStatus {
    fn from(value: QueryStatusItem) -> Self {
        match value {
            QueryStatusItem::Running => Self::Running,
            QueryStatusItem::Successful => Self::Successful,
            QueryStatusItem::Failed => Self::Failed,
            QueryStatusItem::Canceled => Self::Canceled,
            QueryStatusItem::TimedOut => Self::TimedOut,
        }
    }
}

// Keep own QueryRecordId for compatibility with current open api schema
// Currently QueryRecordId used in apu-ui support only i64 based query_id
pub type QueryRecordId = i64;

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

impl TryFrom<core_history::QueryRecord> for QueryRecord {
    type Error = QueryError;

    fn try_from(query: core_history::QueryRecord) -> QueryRecordResult<Self> {
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
            id: query.id.into(),
            worksheet_id: query.worksheet_id,
            query: query.query,
            start_time: query.start_time,
            end_time: query.end_time,
            duration_ms: query.duration_ms,
            result_count: query.result_count,
            status: query.status.into(),
            result: result_set,
            error: query_error,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueriesResponse {
    pub items: Vec<QueryRecord>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetQueriesParams {
    pub worksheet_id: Option<WorksheetId>,
    pub sql_text: Option<String>,     // filter by SQL Text
    pub min_duration_ms: Option<i64>, // filter Duration greater than
    pub cursor: Option<QueryRecordId>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
}

#[allow(clippy::from_over_into)]
impl Into<core_history::GetQueriesParams> for GetQueriesParams {
    fn into(self) -> core_history::GetQueriesParams {
        core_history::GetQueriesParams {
            worksheet_id: self.worksheet_id,
            sql_text: self.sql_text,
            min_duration_ms: self.min_duration_ms,
            cursor: self.cursor.map(Into::into),
            limit: self.limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryGetResponse(pub QueryRecord);
