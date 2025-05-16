use crate::default_limit;
use chrono::NaiveDateTime;
use core_metastore::RwObject;
use core_metastore::models::{Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent};
use serde::{Deserialize, Serialize};
use std::convert::From;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Schema {
    pub name: String,
    pub database: String,
    pub created_at: String,
    pub updated_at: String,
}

impl Schema {
    #[must_use]
    pub fn new(name: String, database: String) -> Self {
        Self {
            name,
            database,
            created_at: chrono::Utc::now().naive_utc().to_string(),
            updated_at: chrono::Utc::now().naive_utc().to_string(),
        }
    }
}

impl From<RwObject<MetastoreSchema>> for Schema {
    fn from(rw_schema: RwObject<MetastoreSchema>) -> Self {
        Self {
            name: rw_schema.data.ident.schema,
            database: rw_schema.data.ident.database,
            created_at: rw_schema.created_at.to_string(),
            updated_at: rw_schema.updated_at.to_string(),
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreatePayload {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdatePayload {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdateResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreateResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemasResponse {
    pub items: Vec<Schema>,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct SchemasParameters {
    pub offset: Option<usize>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
    pub search: Option<String>,
    pub order_by: Option<String>,
    pub order_direction: Option<String>,
}
