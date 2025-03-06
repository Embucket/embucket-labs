use bytes::Bytes;
use icebucket_utils::IterableEntity;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use utoipa::ToSchema;
use chrono::{DateTime, Utc};

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

impl IterableEntity for HistoryItem {
    const SUFFIX_MAX_LEN: usize = 19; //for int64::MAX
    const PREFIX: &[u8] = b"hi.";

    fn key(&self) -> Bytes {
        Self::key_with_prefix(self.start_time.timestamp_nanos_opt().unwrap_or(0))
    }

    fn min_key() -> Bytes {
        Self::key_with_prefix(0)
    }

    fn max_key() -> Bytes {
        Self::key_with_prefix(i64::MAX)
    }
}
