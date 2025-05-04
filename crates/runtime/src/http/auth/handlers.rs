use std::collections::HashMap;

use tracing;
use snafu::ResultExt;
use http::HeaderMap;
use http::header::SET_COOKIE;
use crate::http::auth::error::{AuthResult, AuthError, StoreSnafu, ResponseHeaderSnafu};
use crate::http::auth::models::{Claims, RefreshClaims, LoginPayload, LoginResponse};
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
use super::error::{CreateJwtSnafu, ValidateJwtSnafu};
use http::HeaderName;
use serde::{Deserialize, Serialize};

pub const REFRESH_TOKEN_EXPIRATION_HOURS: i64 = 24*7;

pub fn cookies_from_header (headers: &HeaderMap) -> HashMap<&str, &str> {
    let mut cookies_map = HashMap::new();

    let cookies = headers.get_all(http::header::COOKIE);

    for value in cookies.iter() {
        match value.to_str() {
            Ok(cookie_str) => {
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
    return cookies_map;
}

#[must_use]
fn access_token_claims(username: &String, audience: &String) -> Claims {
    let now = Local::now();
    let iat = now.timestamp();
    let exp = now.timestamp() + Duration::minutes(15).whole_seconds();

    Claims {
        sub: username.clone(),
        aud: audience.clone(),
        iat,
        exp,
    }
}

#[must_use]
fn refresh_token_claims(username: &String, audience: &String) -> RefreshClaims {
    let now = Local::now();
    let iat = now.timestamp();
    let exp = now.timestamp() + Duration::hours(REFRESH_TOKEN_EXPIRATION_HOURS).whole_seconds();

    RefreshClaims {
        sub: username.clone(),
        aud: audience.clone(),
        iat,
        exp,
    }
}

pub fn get_claims_validate_jwt_token(token: &str, audience: &String, jwt_secret: &String) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.leeway = 5;
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["exp", "aud"]);
  
    let decoding_key = DecodingKey::from_secret(jwt_secret.as_bytes());
  
    let decoded = decode::<Claims>(token, &decoding_key, &validation)?;
  
    Ok(decoded.claims)
}

fn create_jwt<T>(claims: &T, jwt_secret: &String) -> Result<String, jsonwebtoken::errors::Error>
where
    T: Serialize {
    let jwt_secret = jwt_secret.as_bytes();
    encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret))
}

fn set_cookies(headers: &mut HeaderMap, refresh_token: &str, access_token: &str) -> AuthResult<()> {
    headers.try_append(
        SET_COOKIE,
        Cookie::build(("refresh_token", refresh_token))
            .http_only(true)
            .secure(true)
            .path("/")
            .to_string()
            .parse()
            .unwrap(),
    ).context(ResponseHeaderSnafu)?;

    headers.try_append(
        SET_COOKIE, 
        Cookie::build(("access_token", access_token))
            .secure(true)
            .path("/")
            .to_string()
            .parse()
            .unwrap(),
    ).context(ResponseHeaderSnafu)?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip(state, password), err)]
pub async fn login(
    State(state): State<AppState>,
    Json(LoginPayload {username, password}): Json<LoginPayload>,
) -> AuthResult<impl IntoResponse> {
    if username != "admin" || password != "admin" {
        return Err(AuthError::Login)
    }

    let audience = &state.config.host;
    let jwt_secret = state.auth_config.jwt_secret();

    let access_token_claims = access_token_claims(&username, audience);

    let access_token = create_jwt(&access_token_claims, jwt_secret)
        .context(CreateJwtSnafu)?;

    let refresh_token_claims = refresh_token_claims(&username, audience);

    let refresh_token = create_jwt(&refresh_token_claims, jwt_secret)
    .context(CreateJwtSnafu)?;

    state.auth_store.put_token(Token::new(username, refresh_token.clone()))
        .await
        .context(StoreSnafu)?;

    let mut headers = HeaderMap::new();
    set_cookies(&mut headers, &refresh_token, &access_token)?;

    Ok((
        headers,
        Json(LoginResponse { access_token }),
    ))
}

pub async fn refresh_access_token(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AuthResult<impl IntoResponse> {

    let cookies_map = cookies_from_header(&headers);

    // return Err(AuthError::Custom { message: format!("cookies: {cookies:?}, cookies_map: {cookies_map:?}") });
    // let refresh_token =cookies.iter().find(|cookie| {
    //     cookie.to_str().unwrap_or_default().starts_with("refresh_token=")
    // });

    match cookies_map.get("refresh_token") {
        None => Err(AuthError::Unauthorized),
        Some(refresh_token) => {
            let audience = &state.config.host;
            let jwt_secret = state.auth_config.jwt_secret();
    
            let refresh_claims = get_claims_validate_jwt_token(refresh_token, audience, jwt_secret)
                .context(ValidateJwtSnafu)?;
    
            let access_claims = access_token_claims(&refresh_claims.sub, audience);

            let access_token = create_jwt(&access_claims, jwt_secret)
                .context(CreateJwtSnafu)?;
    
            let mut headers = HeaderMap::new();
            set_cookies(&mut headers, refresh_token, &access_token)?;

            Ok((
                headers,
                Json(LoginResponse { access_token }),
            ))
        }
    }
}


pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AuthResult<impl IntoResponse> {
    let cookies_map = cookies_from_header(&headers);

    match cookies_map.get("refresh_token") {
        Some(refresh_token) => {
            let audience = &state.config.host;
            let jwt_secret = state.auth_config.jwt_secret();
    
            let claims = get_claims_validate_jwt_token(refresh_token, audience, jwt_secret)
                .context(ValidateJwtSnafu)?;

            // Delete refresh token from storage
            state.auth_store.delete_token(claims.sub)
                .await
                .context(StoreSnafu)?;
        }
        // logout doesn't return unuathorized
        None => (),
    }

    // unset refresh_token, access_token cookies

    let mut headers = HeaderMap::new();
    set_cookies(&mut headers, "", "")?;

    Ok((headers, ()))

}