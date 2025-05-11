use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticWebConfig {
    pub host: String,
    pub port: u16,
}
