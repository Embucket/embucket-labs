use core_metastore::RwObject;
use core_metastore::error as metastore_err;
use core_metastore::models::Database as MetastoreDatabase;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    pub id: i64,
    pub name: String,
    pub volume: String,
    pub created_at: String,
    pub updated_at: String,
}

impl TryFrom<RwObject<MetastoreDatabase>> for Database {
    type Error = super::Error;
    fn try_from(db: RwObject<MetastoreDatabase>) -> Result<Self, Self::Error> {
        Ok(Self {
            id: db
                .id()
                .context(metastore_err::NoIdSnafu)
                .context(super::error::NoIdSnafu)?,
            volume: db.data.volume,
            name: db.data.ident,
            created_at: db.created_at.to_string(),
            updated_at: db.updated_at.to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreatePayload {
    pub name: String,
    pub volume: String,
}

// TODO: make Database fields optional in update payload, not used currently
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdatePayload {
    pub name: String,
    pub volume: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreateResponse(pub Database);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdateResponse(pub Database);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseResponse(pub Database);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabasesResponse {
    pub items: Vec<Database>,
}
