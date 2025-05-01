use bytes::Bytes;
use chrono::{DateTime, Utc};
use embucket_utils::iterable::IterableEntity;
use serde::{Deserialize, Serialize};

pub type TokenId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: TokenId,
    pub token: String,
    pub created_at: DateTime<Utc>,
}

impl Token {
    #[must_use]
    pub fn get_key(id: &TokenId) -> Bytes {
        Bytes::from(format!("/token/{id}"))
    }

    #[must_use]
    pub fn new(id: TokenId, token: String) -> Self {
        Self {
            id,
            token,
            created_at: Utc::now(),
        }
    }
}

impl IterableEntity for Token {
    type Cursor = TokenId;

    fn cursor(&self) -> Self::Cursor {
        self.id.clone()
    }

    fn key(&self) -> Bytes {
        Self::get_key(&self.id)
    }
}
