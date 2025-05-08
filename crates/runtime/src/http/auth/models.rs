use serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(Debug, Deserialize))]
pub struct AuthResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u32,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(Debug, Deserialize))]
pub struct RefreshTokenResponse {
    pub access_token: String,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug))]
pub struct Claims {
    pub sub: String, // token issued to a particular user
    pub aud: String, // validate audience since as it can be deployed on multiple hosts
    pub iat: i64,    // Issued At
    pub exp: i64,    // Expiration Time
}
