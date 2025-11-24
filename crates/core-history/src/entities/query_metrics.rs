use crate::QueryRecordId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetric {
    pub query_id: QueryRecordId,
    pub node_id: usize,
    pub parent_node_id: Option<usize>,
    pub operator: String,
    pub metrics: serde_json::Value, // serialized metrics as JSON object
    pub created_at: DateTime<Utc>,
}

impl QueryMetric {
    #[must_use]
    pub fn new(
        query_id: QueryRecordId,
        node_id: usize,
        parent_node_id: Option<usize>,
        operator: &str,
        metrics: serde_json::Value,
    ) -> Self {
        Self {
            query_id,
            node_id,
            parent_node_id,
            operator: operator.to_string(),
            metrics,
            created_at: Utc::now(),
        }
    }
}
