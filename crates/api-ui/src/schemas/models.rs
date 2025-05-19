use crate::default_limit;
use api_structs::schemas::{
    Schema as SchemaRest, SchemaCreatePayload as SchemaCreatePayloadRest,
    SchemaCreateResponse as SchemaCreateResponseRest,
};
use chrono::NaiveDateTime;
use core_metastore::RwObject;
use core_metastore::models::{Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent};
use serde::{Deserialize, Serialize};
use std::convert::From;
use utoipa::{IntoParams, ToSchema};

pub type Schema = SchemaRest;
pub type SchemaCreatePayload = SchemaCreatePayloadRest;
pub type SchemaCreateResponse = SchemaCreateResponseRest;

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
pub struct SchemaResponse {
    #[serde(flatten)]
    pub data: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemasResponse {
    pub items: Vec<Schema>,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct SchemasParameters {
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
    pub search: Option<String>,
}
