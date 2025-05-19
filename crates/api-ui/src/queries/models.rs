use crate::default_limit;
use core_history::QueryRecordId;
use core_history::WorksheetId;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub type QueryCreatePayload = api_structs::query::QueryCreatePayload;
pub type QueryCreateResponse = api_structs::query::QueryCreateResponse;
pub type QueryRecord = api_structs::query::QueryRecord;
pub type QueryStatus = api_structs::query::QueryStatus;
pub type Column = api_structs::result_set::Column;
pub type ExecutionContext = core_executor::query::QueryContext;
pub type ResultSet = api_structs::result_set::ResultSet;

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
    pub sql_text: Option<String>,     // filter by SQL Text
    pub min_duration_ms: Option<i64>, // filter Duration greater than
    pub cursor: Option<QueryRecordId>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
}

#[allow(clippy::from_over_into)]
impl Into<core_history::GetQueries> for GetQueriesParams {
    fn into(self) -> core_history::GetQueries {
        core_history::GetQueries {
            worksheet_id: self.worksheet_id,
            sql_text: self.sql_text,
            min_duration_ms: self.min_duration_ms,
            cursor: self.cursor,
            limit: self.limit,
        }
    }
}
