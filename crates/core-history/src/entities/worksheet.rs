use bytes::Bytes;
use chrono::{DateTime, Utc};
use core_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};

pub type WorksheetId = i64;

// Worksheet struct is used for storing Query History result and also used in http response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Worksheet {
    pub id: WorksheetId,
    pub name: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Worksheet {
    #[must_use]
    pub fn get_key(id: WorksheetId) -> Bytes {
        Bytes::from(format!("/ws/{id}"))
    }

    #[must_use]
    pub fn new(name: String, content: String) -> Self {
        let created_at = Utc::now();
        let id = created_at.timestamp_micros();
        // id, start_time have the same value
        Self {
            id,
            name,
            content,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_content(&mut self, content: String) {
        self.content = content;
    }

    pub fn set_updated_at(&mut self, updated_at: Option<DateTime<Utc>>) {
        self.updated_at = updated_at.unwrap_or_else(Utc::now);
    }
}

impl IterableEntity for Worksheet {
    type Cursor = WorksheetId;

    fn cursor(&self) -> Self::Cursor {
        self.id
    }

    fn key(&self) -> Bytes {
        Self::get_key(self.id)
    }
}

#[cfg(test)]
mod test {
    use super::Worksheet;

    #[test]
    fn test_new_worksheet() {
        let w1 = Worksheet::new(String::new(), String::new());
        assert_eq!(w1.id, w1.created_at.timestamp_millis());
    }
}
