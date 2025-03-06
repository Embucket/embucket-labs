use serde::{Deserialize, Serialize};

use crate::http::config::IceBucketWebConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceBucketRuntimeConfig {
    pub web: IceBucketWebConfig,
    pub db: IceBucketDbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceBucketDbConfig {
    pub slatedb_prefix: String,
}
