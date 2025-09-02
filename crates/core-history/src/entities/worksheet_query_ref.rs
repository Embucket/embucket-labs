use crate::{QueryRecordId, WorksheetId};
use bytes::Bytes;
use core_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};

// QueryRecordReference struct is used for referencing QueryRecord from worksheet.
// We don't need its fields when deserializing from DB, it's not intended for use after
// deserialized
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct QueryRecordReference {
    #[serde(skip)]
    pub id: QueryRecordId,
    #[serde(skip)]
    pub worksheet_id: WorksheetId,
}

impl QueryRecordReference {
    #[must_use]
    pub fn get_key(worksheet_id: WorksheetId, id: QueryRecordId) -> Bytes {
        // use /rws prefix to avoid clashing with /ws prefix used in worksheet
        Bytes::from(format!("/rws/{worksheet_id}/qh/{id}"))
    }

    pub fn extract_qh_key(data: &Bytes) -> Option<Bytes> {
        let pattern = b"/qh/";
        data.windows(pattern.len())
            .position(|w| w == pattern)
            .map(|pos| data.slice(pos..))
    }
}

impl IterableEntity for QueryRecordReference {
    type Cursor = i64;

    fn cursor(&self) -> Self::Cursor {
        self.id.into()
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.worksheet_id, self.id)
    }
}
