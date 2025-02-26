use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use validator::Validate;

use super::IceBucketVolumeIdent;

/// A database identifier
pub type IceBucketDatabaseIdent = String;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct IceBucketDatabase {
    #[validate(length(min = 1))]
    pub ident: IceBucketDatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: IceBucketVolumeIdent,
}

impl IceBucketDatabase {
    #[must_use] 
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}{}", parent, self.ident)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let db = IceBucketDatabase {
            ident: "db".to_string(),
            properties: None,
            volume: "vol".to_string(),
        };
        assert_eq!(db.prefix("parent"), "parent/db");
    }
}