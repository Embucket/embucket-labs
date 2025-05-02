use serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[cfg_attr(test, derive(Debug, Deserialize))]
pub struct LoginResponse {
    pub access_token: String,
}

#[derive(Serialize)]
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
pub struct Claims {
    pub sub: String, // token issued to a particular user
    pub aud: String, // validate audience since as it can be deployed on multiple hosts
    pub iat: i64, // Issued At
    pub exp: i64, // Expiration Time
}
