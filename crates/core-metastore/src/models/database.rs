use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;
use crate::error::Result;
use super::VolumeIdent;
use super::RwObject;
use super::MAP_VOLUME_ID;

/// A database identifier
pub type DatabaseIdent = String;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    #[validate(length(min = 1))]
    pub ident: DatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    pub volume: VolumeIdent,
}

impl Database {
    #[must_use]
    pub const fn new(ident: DatabaseIdent, volume: VolumeIdent) -> Self {
        Self {
            ident,
            properties: None,
            volume,
        }
    }
    #[must_use]
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident)
    }
}

impl RwObject<Database> {
    #[must_use]
    pub fn with_volume_id(self, id: i64) -> Self {
        self.with_named_id(MAP_VOLUME_ID.to_string(), id)
    }

    pub fn volume_id(&self) -> Result<i64> {
        self.named_id(MAP_VOLUME_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let db = Database::new("db".to_string(), "volume".to_string());
        assert_eq!(db.prefix("parent"), "parent/db".to_string());
    }
}
