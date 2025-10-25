use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::DatabaseIdent;

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

impl Schema {
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
        let schema = Schema {
            ident: SchemaIdent {
                schema: "schema".to_string(),
                database: "db".to_string(),
            },
            properties: None,
        };
        assert_eq!(schema.prefix("parent"), "parent/schema");
    }
}
