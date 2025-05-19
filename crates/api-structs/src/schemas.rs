use super::with_derives;
use chrono::NaiveDateTime;
use core_metastore::RwObject;
use core_metastore::models::{Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;

with_derives! {
    #[derive(Debug, Clone)]
    pub struct Schema {
        pub name: String,
        pub database: String,
        pub created_at: NaiveDateTime,
        pub updated_at: NaiveDateTime,
    }
}

impl Schema {
    #[must_use]
    pub fn new(name: String, database: String) -> Self {
        Self {
            name,
            database,
            created_at: chrono::Utc::now().naive_utc(),
            updated_at: chrono::Utc::now().naive_utc(),
        }
    }
}

impl From<RwObject<MetastoreSchema>> for Schema {
    fn from(rw_schema: RwObject<MetastoreSchema>) -> Self {
        Self {
            name: rw_schema.data.ident.schema,
            database: rw_schema.data.ident.database,
            created_at: rw_schema.created_at,
            updated_at: rw_schema.updated_at,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<MetastoreSchema> for Schema {
    fn into(self) -> MetastoreSchema {
        MetastoreSchema {
            ident: MetastoreSchemaIdent {
                schema: self.name,
                database: self.database,
            },
            properties: None,
        }
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct SchemaCreatePayload {
        pub name: String,
    }
}

with_derives! {
    #[derive(Debug, Clone)]
    pub struct SchemaCreateResponse {
        #[serde(flatten)]
        pub data: Schema,
    }
}
