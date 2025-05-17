use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(Debug, Deserialize))]
pub struct RefreshTokenResponse {
    pub access_token: String,
}

#[derive(Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(test, derive(Serialize))]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccountResponse {
    pub username: String,
}

// For internal use, should not be included into open api spec
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug))]
pub struct Claims {
    pub sub: String, // token issued to a particular user
    pub aud: String, // validate audience since as it can be deployed on multiple hosts
    pub iat: i64,    // Issued At
    pub exp: i64,    // Expiration Time
}
