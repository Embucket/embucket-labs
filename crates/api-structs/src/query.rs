use super::with_derives;
#[cfg(feature = "schema")] use utoipa::ToSchema;
#[cfg(feature = "serde")]  use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]  use serde_json::Value;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

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

with_derives! {
    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        pub r#type: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "schema", schema(as = Row, value_type = Vec<Value>))]
    pub struct Row(Vec<Value>);
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct ResultSet {
        pub columns: Vec<Column>,
        pub rows: Vec<Row>,
    }
}