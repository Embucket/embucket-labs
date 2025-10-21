use super::error::{QueryError, QueryRecordResult, QueryStatusParseSnafu, ResultParseSnafu};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
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

impl TryFrom<core_history::ResultSet> for ResultSet {
    type Error = QueryError;

    fn try_from(result_set: core_history::ResultSet) -> QueryRecordResult<Self> {
        Ok(Self {
            columns: result_set
                .columns
                .into_iter()
                .map(|col| Column {
                    name: col.name,
                    r#type: col.r#type,
                })
                .collect(),
            rows: result_set.rows.into_iter().map(|row| Row(row.0)).collect(),
        })
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

impl TryFrom<&str> for QueryStatus {
    type Error = QueryError;

    fn try_from(value: &str) -> QueryRecordResult<Self> {
        match value.to_lowercase().as_str() {
            "running" => Ok(Self::Running),
            "successful" => Ok(Self::Successful),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            "timedout" => Ok(Self::TimedOut),
            other => QueryStatusParseSnafu {
                got: other.to_string(),
            }
            .fail(),
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
    pub result_id: Option<String>,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: i64,
    pub result_count: i64,
    pub status: QueryStatus,
    pub error: String, // empty error - ok
}

impl TryFrom<core_history::QueryRecord> for QueryRecord {
    type Error = QueryError;

    fn try_from(query: core_history::QueryRecord) -> QueryRecordResult<Self> {
        let query_error = query.error.unwrap_or_default();
        Ok(Self {
            id: query.id.into(),
            worksheet_id: query.worksheet_id,
            result_id: query.result_id,
            query: query.query,
            start_time: query.start_time,
            end_time: query.end_time,
            duration_ms: query.duration_ms,
            result_count: query.result_count,
            status: query.status.into(),
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
    pub min_duration_ms: Option<i64>, // filter Duration greater than
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryGetResponse(pub QueryRecord);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultGetResponse(pub ResultSet);
