use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::VolumeIdent;
use uuid::Uuid;

/// A database identifier
pub type DatabaseIdent = String;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    #[validate(length(min = 1))]
    pub ident: DatabaseIdent,
    // pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: VolumeIdent,
}

impl Database {
    pub fn new(ident: DatabaseIdent, volume: VolumeIdent) -> Self {
        Self {
            // ident: Uuid::new_v4(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let db = Database::new("db".to_string(), "vol".to_string());
        assert_eq!(db.prefix("parent"), "parent/db".to_string());
    }
}
