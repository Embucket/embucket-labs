use super::with_derives;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use utoipa::ToSchema;

with_derives! {
    #[derive(Clone)]
    pub struct LoginPayload {
        pub username: String,
        pub password: String,
    }
}

with_derives! {
    #[derive(Clone)]
    pub struct AuthResponse {
        pub access_token: String,
        pub token_type: String,
        pub expires_in: u32,
    }
}

impl AuthResponse {
    #[must_use]
    pub fn new(access_token: String, expires_in: u32) -> Self {
        Self {
            access_token,
            token_type: "Bearer".to_string(),
            expires_in,
        }
    }
}
