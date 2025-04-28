use serde::{Deserialize, Serialize};

use crate::http::config::WebConfig;
use crate::http::web_assets::config::StaticWebConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub web: WebConfig,
    pub web_assets: StaticWebConfig,
    pub db: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub slatedb_prefix: String,
}
