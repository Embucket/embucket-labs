#[cfg(feature = "schema")] use utoipa::ToSchema;
#[cfg(feature = "serde")]  use serde::{Deserialize, Serialize};

#[derive(Clone)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "partial", derive(PartialEq))]
#[cfg_attr(feature = "eq", derive(Eq))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

#[derive(Clone)]
#[cfg_attr(feature = "schema", derive(ToSchema))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "partial", derive(PartialEq))]
#[cfg_attr(feature = "eq", derive(Eq))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
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
