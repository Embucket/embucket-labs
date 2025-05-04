use std::collections::HashMap;

use datafusion::parquet::data_type::AsBytes;
use tracing;
use snafu::ResultExt;
use http::HeaderMap;
use http::header::SET_COOKIE;
use crate::http::auth::error::{AuthResult, AuthError, RandSnafu, StoreSnafu, ResponseHeaderSnafu};
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
use axum::response::IntoResponse;
use axum::response::IntoResponseParts;
use super::error::{CreateJwtSnafu, ValidateJwtSnafu};
use http::HeaderName;

fn create_refresh_token() -> Result<String, <OsRng as TryRngCore>::Error> {
    let mut bytes = [0u8; 32]; // 256 bits
    OsRng.try_fill_bytes(&mut bytes)?; // Secure random bytes
    Ok(general_purpose::URL_SAFE_NO_PAD.encode(&bytes)) // Base64 URL-safe
}

pub fn validate_jwt_token(token: &str, audience: &String, jwt_secret: &String) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.leeway = 5;
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["sub", "exp", "iat", "aud"]);
  
    let decoding_key = DecodingKey::from_secret(jwt_secret.as_bytes());
  
    let decoded = decode::<Claims>(token, &decoding_key, &validation)?;
  
    Ok(decoded.claims)
}

fn create_jwt(username: &String, audience: &String, jwt_secret: &String) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Local::now();
    let iat = now.timestamp();
    let exp = now.timestamp() + Duration::minutes(15).whole_seconds();

    let claims = Claims {
        sub: username.clone(),
        aud: audience.clone(),
        iat,
        exp,
    };
    let jwt_secret = jwt_secret.as_bytes();
    encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret))
}

#[tracing::instrument(level = "debug", skip(state, password), err)]
pub async fn login(
    State(state): State<AppState>,
    Json(LoginPayload {username, password}): Json<LoginPayload>,
) -> AuthResult<impl IntoResponse> {
    if username != "admin" || password != "admin" {
        // return Err(Box::new(AuthError::Login))
        return Err(AuthError::Login)
    }

    let audience = &state.config.host;
    let jwt_secret = state.auth_config.jwt_secret();

    let access_token = create_jwt(&username, &audience, jwt_secret)
        .context(CreateJwtSnafu)?;

    // TODO: encode refresh token so in /refresh it could be validated
    let refresh_token = create_refresh_token()
        .context(RandSnafu)?;

    state.auth_store.add_token(Token::new(username, refresh_token.clone()))
        .await
        .context(StoreSnafu)?;

    let cookie = Cookie::build(("refresh_token", refresh_token))
        .http_only(true)
        .secure(true)
        .path("/refresh");

    let mut headers = HeaderMap::new();
    headers.try_insert(SET_COOKIE, cookie.to_string().parse().unwrap())
        .context(ResponseHeaderSnafu)?;

    Ok((
        headers,
        Json(LoginResponse { access_token }),
    ))
}

pub async fn refresh_access_token(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AuthResult<impl IntoResponse> {
    let cookies = headers.get_all(http::header::COOKIE);

    let mut cookies_map = HashMap::new();

    for value in cookies.iter() {
        match value.to_str() {
            Ok(cookie_str) => {
                println!("Cookie header: {}", cookie_str);
                // parse separate cookes
                for cookie in cookie_str.split(';') {
                    let parts: Vec<_> = cookie.trim().split('=').collect();
                    cookies_map.insert(parts[0], parts[1]);
                }
            }
            Err(_) => {
                eprintln!("Invalid UTF-8 in cookie header");
            }
        }
    }

    // return Err(AuthError::Custom { message: format!("cookies: {cookies:?}, cookies_map: {cookies_map:?}") });
    // let refresh_token =cookies.iter().find(|cookie| {
    //     cookie.to_str().unwrap_or_default().starts_with("refresh_token=")
    // });

    let refresh_token = cookies_map.get("refresh_token");
    match refresh_token {
        None => Err(AuthError::Unauthorized),
        Some(refresh_token) => {
            let audience = &state.config.host;
            let jwt_secret = state.auth_config.jwt_secret();
    
            // TODO: ensure refresh token encoded in a right way 
            let claims = validate_jwt_token(refresh_token, audience, jwt_secret)
                .context(ValidateJwtSnafu)?;
    
            let Claims { sub: username, aud: audience, .. } = claims;
            let access_token = create_jwt(&username, &audience, jwt_secret)
                .context(CreateJwtSnafu)?;
    
            Ok(Json(LoginResponse { access_token }))
        }
    }
}
