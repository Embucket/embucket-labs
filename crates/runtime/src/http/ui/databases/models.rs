use crate::http::ui::default_limit;
use embucket_metastore::models::Database as MetastoreDatabase;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct Database {
    pub name: String,
    pub volume: String,
}

impl From<MetastoreDatabase> for Database {
    fn from(db: MetastoreDatabase) -> Self {
        Self {
            name: db.ident,
            volume: db.volume,
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreatePayload {
    #[serde(flatten)]
    pub data: Database,
}

// TODO: make Database fields optional in update payload, not used currently
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdatePayload {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreateResponse {
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
