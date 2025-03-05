use utils::{Db, IterableEntity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HistoryItem {
    pub id: Uuid,
    pub query: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status_code: u16,
}

impl IterableEntity for HistoryItem {
    const SUFFIX_MAX_LEN: usize = 19; //for int64::MAX
    const PREFIX: &[u8] = b"hi.";

    fn key(&self) -> Bytes {
        Self::concat_with_prefix(self.start_time.timestamp_nanos_opt().unwrap_or(0))
    }

    fn min_key() -> Bytes {
        Self::concat_with_prefix(0)
    }

    fn max_key() -> Bytes {
        Self::concat_with_prefix(i64::MAX)
    }
}
