use tracing;
use snafu::ResultExt;
use http::HeaderMap;
use http::header::SET_COOKIE;
use crate::http::auth::error::{AuthResult, AuthError, RandSnafu, StoreSnafu};
use crate::http::auth::models::{Claims, LoginPayload, LoginResponse};
use crate::http::state::AppState;
use axum::{Json};
use axum::extract::{State};
use time::Duration;
use jsonwebtoken::{encode, decode, Header, DecodingKey, EncodingKey, Validation};
use chrono::offset::Local;
use rand::rand_core::{TryRngCore, OsRng};
use base64::{engine::general_purpose, Engine as _};
use embucket_history::Token;
use tower_sessions::cookie::Cookie;

use super::error::JwtSnafu;

fn create_refresh_token() -> Result<String, <OsRng as TryRngCore>::Error> {
    let mut bytes = [0u8; 32]; // 256 bits
    OsRng.try_fill_bytes(&mut bytes)?; // Secure random bytes
    Ok(general_purpose::URL_SAFE_NO_PAD.encode(&bytes)) // Base64 URL-safe
}

pub fn validate_jwt_token(token: &str, audience: &str, jwt_secret: &[u8]) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.leeway = 5;
    // Setting audience
    validation.set_audience(&[audience]);
    // Setting required claims
    validation.set_required_spec_claims(&["exp", "aud"]);
  
    let decoding_key = DecodingKey::from_secret(jwt_secret);
  
    //Throws error if invalid
    let decoded = decode::<Claims>(token, &decoding_key, &validation)?;
  
    Ok(decoded.claims)
}

fn create_jwt(username: &String, audience: &String, jwt_secret: &[u8]) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Local::now();
    let iat = now.timestamp();
    let exp = now.timestamp() + Duration::minutes(15).whole_seconds();

    let claims = Claims {
        sub: username.clone(),
        aud: audience.clone(),
        iat,
        exp,
    };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret))
}

#[tracing::instrument(level = "debug", skip(state, password), err, ret(level = tracing::Level::TRACE))]
pub async fn login(
    State(state): State<AppState>,
    Json(LoginPayload {username, password}): Json<LoginPayload>,
) -> AuthResult<(HeaderMap, Json<LoginResponse>)> {
    if username != "admin" || password != "admin" {
        return Err(Box::new(AuthError::Login))
    }

    let audience = &state.config.host;

    let jwt_secret = state.auth_config.jwt_secret();

    let access_token = create_jwt(&username, &audience, jwt_secret.as_bytes())
        .context(JwtSnafu)?;

    let refresh_token = create_refresh_token()
        .context(RandSnafu)?;

    state.auth_store.add_token(Token::new(username, refresh_token.clone()))
        .await
        .context(StoreSnafu)?;

    let cookie = Cookie::build(("refresh_token", refresh_token))
        .http_only(true)
        .path("/refresh");

    let mut headers = HeaderMap::new();
    headers.try_insert(SET_COOKIE, cookie.to_string().parse().unwrap())?;

    Ok((headers, Json(LoginResponse { access_token })))
}
