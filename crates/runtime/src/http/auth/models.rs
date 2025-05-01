use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub access_token: String,
}

#[derive(Deserialize)]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // Subject (whom token refers to)
    pub aud: String, // Audience (whom token is intended for)
    pub iat: i64, // Issued At
    pub exp: i64, // Expiration Time
}
