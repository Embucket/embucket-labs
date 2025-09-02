pub mod error;
pub mod handlers;
pub mod router;
pub mod state;

use core_history::QueryRecordId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum QueryIdParam {
    I64(i64),
    Uuid(uuid::Uuid),
}

impl Into<QueryRecordId> for QueryIdParam {
    fn into(self) -> QueryRecordId {
        match self {
            QueryIdParam::I64(i64) => QueryRecordId::from(i64),
            QueryIdParam::Uuid(uuid) => QueryRecordId::from(uuid),
        }
    }
}