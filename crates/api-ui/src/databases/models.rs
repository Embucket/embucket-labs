use crate::default_limit;
use api_structs::databases::{
    Database as DatabaseRest, DatabaseCreatePayload as DatabaseCreatePayloadRest,
    DatabaseCreateResponse as DatabaseCreateResponseRest,
};
use core_metastore::models::Database as MetastoreDatabase;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

pub type Database = DatabaseRest;
pub type DatabaseCreatePayload = DatabaseCreatePayloadRest;
pub type DatabaseCreateResponse = DatabaseCreateResponseRest;

// TODO: make Database fields optional in update payload, not used currently
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdatePayload {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdateResponse {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseResponse {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabasesResponse {
    pub items: Vec<Database>,
    pub current_cursor: Option<String>,
    pub next_cursor: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DatabasesParameters {
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
    pub search: Option<String>,
}
