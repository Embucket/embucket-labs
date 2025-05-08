#![allow(clippy::missing_const_for_fn)]

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

// Non serializable, no Clone, Copy, Debug traits
#[derive(Default)]
pub struct AuthConfig {
    jwt_secret: String,
    demo_user: String,
    demo_password: String,
}

impl AuthConfig {
    #[must_use]
    pub fn new(jwt_secret: String) -> Self {
        Self {
            jwt_secret,
            ..Self::default()
        }
    }

    pub fn with_demo_credentials(&mut self, demo_user: String, demo_password: String) {
        self.demo_user = demo_user;
        self.demo_password = demo_password;
    }

    #[must_use]
    pub fn jwt_secret(&self) -> &str {
        &self.jwt_secret
    }

    #[must_use]
    pub fn demo_user(&self) -> &str {
        &self.demo_user
    }

    #[must_use]
    pub fn demo_password(&self) -> &str {
        &self.demo_password
    }
}
