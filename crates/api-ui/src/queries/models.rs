use crate::default_limit;
use api_structs::{query::QueryRecord};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use core_history::QueryRecordId;
use core_history::WorksheetId;

pub type ExecutionContext = core_executor::query::QueryContext;


// impl From<QueryStatusItem> for QueryStatus {
//     fn from(value: QueryStatusItem) -> Self {
//         match value {
//             QueryStatusItem::Running => Self::Running,
//             QueryStatusItem::Successful => Self::Successful,
//             QueryStatusItem::Failed => Self::Failed,
//         }
//     }
// }

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
