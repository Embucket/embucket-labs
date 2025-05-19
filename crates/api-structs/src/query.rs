use super::result_set::ResultSet;
use super::with_derives;
use chrono::{DateTime, Utc};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(feature = "schema")]
use utoipa::ToSchema;

with_derives! {
    #[derive(Clone, Debug)]
    pub struct QueryCreatePayload {
        pub worksheet_id: Option<i64>,
        pub query: String,
        pub context: Option<HashMap<String, String>>,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct QueryCreateResponse {
        #[serde(flatten)]
        pub data: QueryRecord,
    }
}

// QueryRecord used by REST
with_derives! {
    #[derive(Debug, Clone)]
    pub struct QueryRecord {
        pub id: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub worksheet_id: Option<i64>,
        pub query: String,
        pub start_time: DateTime<Utc>,
        pub end_time: DateTime<Utc>,
        pub duration_ms: i64,
        pub result_count: i64,
        pub result: ResultSet,
        pub status: QueryStatus,
        pub error: String, // empty error - ok
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub enum QueryStatus {
        Running,
        Successful,
        Failed,
    }
}
