use crate::{QueryRecordId, WorksheetId};
use bytes::Bytes;
use embucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};

// QueryRecordReference struct is used for referencing QueryRecord from worksheet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRecordReference {
    #[serde(skip_serializing)]
    pub id: QueryRecordId,
    #[serde(skip_serializing)]
    pub worksheet_id: WorksheetId,
}

impl QueryRecordReference {
    #[must_use]
    pub fn get_key(worksheet_id: WorksheetId, id: QueryRecordId) -> Bytes {
        Bytes::from(format!("/ws/{worksheet_id}/qh/{id}"))
    }

    pub fn extract_qh_key(data: &Bytes) -> Option<Bytes> {
        let pattern = b"/qh/";
        data.windows(pattern.len())
            .position(|w| w == pattern)
            .map(|pos| data.slice(pos..))
    }
}

impl IterableEntity for QueryRecordReference {
    type Cursor = QueryRecordId;

    fn cursor(&self) -> Self::Cursor {
        self.id
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.worksheet_id, self.id)
    }
}
