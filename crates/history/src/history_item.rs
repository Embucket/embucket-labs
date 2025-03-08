use chrono::{DateTime, Utc};
use icebucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

// HistoryItem struct is used for storing Query History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryItem {
    pub id: Uuid,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status_code: u16,
    pub error: Option<String>,
}

#[allow(clippy::trait_duplication_in_bounds)]
impl IterableEntity for HistoryItem {
    type Cursor = i64;
    const PREFIX: &[u8] = b"hi.";

    fn cursor(&self) -> Self::Cursor {
        self.start_time.timestamp_nanos_opt().unwrap_or(0)
    }

    fn next_cursor(&self) -> Self::Cursor {
        self.cursor() + 1
    }
}
