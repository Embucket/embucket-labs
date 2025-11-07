use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::DatabaseIdent;
use super::{DatabaseId, MAP_SCHEMA_ID, NamedId, RwObject};
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaId(pub i64);

impl NamedId for SchemaId {
    fn type_name() -> &'static str {
        MAP_SCHEMA_ID
    }
}

impl std::ops::Deref for SchemaId {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::from_over_into)]
impl Into<i64> for SchemaId {
    fn into(self) -> i64 {
        self.0
    }
}

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// A schema identifier
#[derive(Default)]
pub struct SchemaIdent {
    #[validate(length(min = 1))]
    /// The name of the schema
    pub schema: String,
    #[validate(length(min = 1))]
    /// The database the schema belongs to
    pub database: DatabaseIdent,
}

impl SchemaIdent {
    #[must_use]
    pub const fn new(database: DatabaseIdent, schema: String) -> Self {
        Self { schema, database }
    }

    #[must_use]
    pub fn normalized(&self) -> Self {
        Self {
            schema: self.schema.to_ascii_lowercase(),
            database: self.database.to_ascii_lowercase(),
        }
    }
}

impl std::fmt::Display for SchemaIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.schema)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Schema {
    pub ident: SchemaIdent,
    pub properties: Option<HashMap<String, String>>,
}

impl RwObject<Schema> {
    #[must_use]
    pub fn with_id(self, id: SchemaId) -> Self {
        self.with_named_id(SchemaId::type_name(), *id)
    }

    pub fn id(&self) -> Result<SchemaId> {
        self.named_id(SchemaId::type_name()).map(SchemaId)
    }

    #[must_use]
    pub fn with_database_id(self, id: DatabaseId) -> Self {
        self.with_named_id(DatabaseId::type_name(), *id)
    }

    pub fn database_id(&self) -> Result<DatabaseId> {
        self.named_id(DatabaseId::type_name()).map(DatabaseId)
    }
}

impl Schema {
    #[must_use]
    pub const fn new(ident: SchemaIdent) -> Self {
        Self {
            ident,
            properties: None,
        }
    }

    #[must_use]
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident.schema)
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.ident.database, self.ident.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let schema = Schema::new(SchemaIdent {
            schema: "schema".to_string(),
            database: "db".to_string(),
        });
        assert_eq!(schema.prefix("parent"), "parent/schema");
    }
}
