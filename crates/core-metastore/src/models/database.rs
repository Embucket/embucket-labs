use std::collections::HashMap;

use crate::error::Result;
use serde::{Deserialize, Serialize};
use validator::Validate;
use super::VolumeIdent;
use super::{MAP_DATABASE_ID, RwObject, NamedId, VolumeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseId(pub i64);

impl NamedId for DatabaseId {
    fn type_name() -> &'static str {
        MAP_DATABASE_ID
    }
}

impl std::ops::Deref for DatabaseId {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::from_over_into)]
impl Into<i64> for DatabaseId {
    fn into(self) -> i64 {
        self.0
    }
}

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
    pub fn with_id(self, id: DatabaseId) -> Self {
        self.with_named_id(DatabaseId::type_name(), id.into())
    }

    pub fn id(&self) -> Result<DatabaseId> {
        self.named_id(DatabaseId::type_name()).map(DatabaseId)
    }
    
    #[must_use]
    pub fn with_volume_id(self, id: VolumeId) -> Self {
        self.with_named_id(VolumeId::type_name(), id.into())
    }

    pub fn volume_id(&self) -> Result<VolumeId> {
        self.named_id(VolumeId::type_name()).map(VolumeId)
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
