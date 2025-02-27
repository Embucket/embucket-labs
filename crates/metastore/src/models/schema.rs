use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::IceBucketDatabaseIdent;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
/// A schema identifier
pub struct IceBucketSchemaIdent {
    #[validate(length(min = 1))]
    /// The name of the schema
    pub schema: String,
    #[validate(length(min = 1))]
    /// The database the schema belongs to
    pub database: IceBucketDatabaseIdent,
}

impl IceBucketSchemaIdent {
    #[must_use]
    pub const fn new(schema: String, database: IceBucketDatabaseIdent) -> Self {
        Self {
            schema,
            database,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, utoipa::ToSchema)]
pub struct IceBucketSchema {
    pub ident: IceBucketSchemaIdent,
    pub properties: Option<HashMap<String, String>>,
}

impl IceBucketSchema {
    #[must_use] 
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.ident.schema)
    }
}

impl std::fmt::Display for IceBucketSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.ident.database, self.ident.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let schema = IceBucketSchema {
            ident: IceBucketSchemaIdent {
                schema: "schema".to_string(),
                database: "db".to_string(),
            },
            properties: None,
        };
        assert_eq!(schema.prefix("parent"), "parent/schema");
    }
}