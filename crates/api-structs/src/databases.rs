use super::with_derives;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;

use core_metastore::models::Database as MetastoreDatabase;

with_derives! {
    #[derive(Debug, Clone)]
    pub struct Database {
        pub name: String,
        pub volume: String,
    }
}

impl From<MetastoreDatabase> for Database {
    fn from(db: MetastoreDatabase) -> Self {
        Self {
            name: db.ident,
            volume: db.volume,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<MetastoreDatabase> for Database {
    fn into(self) -> MetastoreDatabase {
        MetastoreDatabase {
            ident: self.name,
            volume: self.volume,
            properties: None,
        }
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct DatabaseCreatePayload {
        #[serde(flatten)]
        pub data: Database,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct DatabaseCreateResponse {
        #[serde(flatten)]
        pub data: Database,
    }
}
