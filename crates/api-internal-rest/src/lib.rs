pub mod error;
pub mod handlers;
pub mod router;
pub mod state;

use core_history::QueryRecordId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum QueryIdParam {
    I64(i64),
    Uuid(uuid::Uuid),
}

#[allow(clippy::from_over_into)]
impl Into<QueryRecordId> for QueryIdParam {
    fn into(self) -> QueryRecordId {
        match self {
            Self::I64(i64) => QueryRecordId::from(i64),
            Self::Uuid(uuid) => QueryRecordId::from(uuid),
        }
    }
}
