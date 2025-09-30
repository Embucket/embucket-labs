use core_metastore::RwObject;
use core_metastore::models::Schema as MetastoreSchema;
use serde::{Deserialize, Serialize};
use std::convert::From;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    pub name: String,
    pub database: String,
    pub created_at: String,
    pub updated_at: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreatePayload {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdatePayload {
    pub name: String,
    pub database: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaUpdateResponse(pub Schema);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCreateResponse(pub Schema);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResponse(pub Schema);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemasResponse {
    pub items: Vec<Schema>,
}
