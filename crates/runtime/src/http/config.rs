use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceBucketWebConfig {
    pub host: String,
    pub port: u16,
    pub allow_origin: Option<String>,
    pub data_format: String,
}
