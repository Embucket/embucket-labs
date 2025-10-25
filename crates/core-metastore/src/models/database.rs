use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::VolumeIdent;

/// A database identifier
pub type DatabaseIdent = String;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    #[validate(length(min = 1))]
    pub ident: DatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: VolumeIdent,
}

impl Database {
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
        let db = Database {
            ident: "db".to_string(),
            properties: None,
            volume: "vol".to_string(),
        };
        assert_eq!(db.prefix("parent"), "parent/db");
    }
}
