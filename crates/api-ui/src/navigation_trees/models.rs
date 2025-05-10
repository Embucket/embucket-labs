use crate::default_limit;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NavigationTreesResponse {
    pub items: Vec<NavigationTreeDatabase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NavigationTreeDatabase {
    pub name: String,
    pub schemas: Vec<NavigationTreeSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NavigationTreeSchema {
    pub name: String,
    pub tables: Vec<NavigationTreeTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NavigationTreeTable {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct NavigationTreesParameters {
    pub offset: Option<usize>,
    #[serde(default = "default_limit")]
    pub limit: Option<u16>,
}
