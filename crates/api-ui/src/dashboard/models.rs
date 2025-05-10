use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardResponse {
    #[serde(flatten)]
    pub(crate) data: Dashboard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub total_databases: usize,
    pub total_schemas: usize,
    pub total_tables: usize,
    pub total_queries: usize,
}
