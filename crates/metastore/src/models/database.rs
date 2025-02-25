use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use validator::{Validate, ValidationError, ValidationErrors};
use crate::error::{self as metastore_error, MetastoreResult};

use super::{IceBucketVolume, IceBucketVolumeIdent};

/// A database identifier
pub type IceBucketDatabaseIdent = String;

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IceBucketDatabase {
    #[validate(length(min = 1))]
    pub name: IceBucketDatabaseIdent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    /// Volume identifier
    pub volume: IceBucketVolumeIdent,
}

impl IceBucketDatabase {
    #[must_use] 
    pub fn prefix(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix() {
        let db = IceBucketDatabase {
            name: "db".to_string(),
            properties: None,
            volume: "vol".to_string(),
        };
        assert_eq!(db.prefix("parent"), "parent/db");
    }
}