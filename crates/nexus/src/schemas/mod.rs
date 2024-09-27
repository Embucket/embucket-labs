use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub mod storage_profiles;
pub mod warehouses;
pub mod namespaces;


#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
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