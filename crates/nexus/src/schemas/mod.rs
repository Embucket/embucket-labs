use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

pub mod namespaces;
pub mod storage_profiles;
pub mod tables;
pub mod warehouses;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Config {
    pub overrides: HashMap<String, String>,
    pub defaults: HashMap<String, String>,
}

impl From<Config> for catalog::models::Config {
    fn from(config: Config) -> Self {
        catalog::models::Config {
            overrides: config.overrides,
            defaults: config.defaults,
        }
    }
}

impl From<catalog::models::Config> for Config {
    fn from(config: catalog::models::Config) -> Self {
        Config {
            overrides: config.overrides,
            defaults: config.defaults,
        }
    }
}
