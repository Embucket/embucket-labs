#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::auth::error::*;
use crate::auth::handlers::{create_jwt, get_claims_validate_jwt_token, jwt_claims};
use crate::auth::models::{AccountResponse, AuthResponse, LoginPayload};
use crate::queries::models::{QueryCreatePayload, QueryCreateResponse};
use crate::tests::common::{TestHttpError, http_req_with_headers};
use crate::tests::server::run_test_server_with_demo_auth;
use core_metastore::RwObject;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use serde_json::json;
use snafu::location;
use std::collections::HashMap;
use std::net::SocketAddr;
use time::Duration;

const JWT_SECRET: &str = "test";
const DEMO_USER: &str = "demo_user";
const DEMO_PASSWORD: &str = "demo_password";

pub type RwObjectVec<T> = Vec<RwObject<T>>;

#[allow(clippy::explicit_iter_loop)]
fn get_set_cookie_from_response_headers(
    headers: &HeaderMap,
) -> HashMap<&str, (&str, &HeaderValue)> {
    let set_cookies = headers.get_all("Set-Cookie");

    let mut set_cookies_map = HashMap::new();

    for value in set_cookies.iter() {
        let name_values = value.to_str().unwrap().split('=').collect::<Vec<_>>();
        let cookie_name = name_values[0];
        let cookie_values = name_values[1].split("; ").collect::<Vec<_>>();
        let cookie_val = cookie_values[0];
        set_cookies_map.insert(cookie_name, (cookie_val, value));
    }
    set_cookies_map
}

async fn login<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    username: &str,
    password: &str,
) -> std::result::Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/ui/auth/login"),
        json!(LoginPayload {
            username: String::from(username),
            password: String::from(password),
        })
        .to_string(),
    )
    .await
}

async fn logout<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
) -> std::result::Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/ui/auth/logout"),
        String::new(),
    )
    .await
}

async fn refresh<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    refresh_token: &str,
) -> std::result::Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::COOKIE,
                HeaderValue::from_str(format!("refresh_token={refresh_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/auth/refresh"),
        String::new(),
    )
    .await
}

async fn query<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &String,
    query: &str,
) -> std::result::Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: None,
            async_exec: false,
            query: query.to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_login_no_secret_set() {
    // No secret set
    let addr = run_test_server_with_demo_auth(
        String::new(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    let login_error = login::<()>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect_err("Login should fail");
    assert_eq!(login_error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_bad_login() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    let login_error = login::<()>(&client, &addr, "", "")
        .await
        .expect_err("Login should fail");

    let www_auth = login_error
        .headers
        .get(header::WWW_AUTHENTICATE)
        .expect("No WWW-Authenticate header");
    assert_eq!(
        www_auth.to_str().expect("Bad header encoding"),
        "Bearer realm=\"login\", error=\"Login error\""
    );

    let login_error_headers = &login_error.headers;
    let set_cookies = get_set_cookie_from_response_headers(login_error_headers);
    // shouldn't have access_token on bad login
    assert!(!set_cookies.contains_key("access_token"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_query_request_unauthorized() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    let _ = login::<()>(&client, &addr, "", "")
        .await
        .expect_err("Login should fail");

    // Unauthorized error while running query
    let query_err = query::<()>(&client, &addr, &"xyz".to_string(), "SELECT 1")
        .await
        .expect_err("Query should fail");
    assert_eq!(query_err.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_query_request_ok() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    // login
    let (resp_headers, login_response) =
        login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
            .await
            .expect("Failed to login");

    let set_cookies = get_set_cookie_from_response_headers(&resp_headers);
    let _ = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    // Successfuly run query
    let (query_resp_headers, query_response) =
        query::<QueryCreateResponse>(&client, &addr, &login_response.access_token, "SELECT 1")
            .await
            .expect("Failed to run query");

    let set_cookies = get_set_cookie_from_response_headers(&query_resp_headers);
    let _ = set_cookies
        .get("session_id")
        .expect("No Set-Cookie found with session_id");

    assert_eq!(query_response.0.query, "SELECT 1");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_refresh_bad_token() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    let refresh_err = refresh::<()>(&client, &addr, "xyz")
        .await
        .expect_err("Refresh should fail");

    assert_eq!(refresh_err.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_logout() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    // login ok
    let (headers, _) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");
    assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

    // logout ok
    let (headers, ()) = logout::<()>(&client, &addr)
        .await
        .expect("Failed to logout");

    // empty refresh_token cookie set
    let set_cookies = get_set_cookie_from_response_headers(&headers);
    let (refresh_token, _) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert_eq!(refresh_token, &"");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_login_refresh() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    // login
    let (headers, login_response) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (refresh_token, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("HttpOnly")
    );
    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("Secure")
    );
    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("SameSite=Strict")
    );

    // Successfuly run query
    let (_, query_response) =
        query::<QueryCreateResponse>(&client, &addr, &login_response.access_token, "SELECT 1")
            .await
            .expect("Failed to run query");
    assert_eq!(query_response.0.query, "SELECT 1");

    //
    // test refresh handler, using refresh_token from cookie from login
    //
    let (headers, _) = refresh::<AuthResponse>(&client, &addr, refresh_token)
        .await
        .expect("Refresh request failed");

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (_, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("HttpOnly")
    );
    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("Secure")
    );
    assert!(
        refresh_token_cookie
            .to_str()
            .expect("Bad cookie")
            .contains("SameSite=Strict")
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_jwt_token_expired() {
    let username = DEMO_USER;
    let audience = "localhost";
    let claims = jwt_claims(username, audience, Duration::seconds(-10));
    let expired_token = create_jwt(&claims, JWT_SECRET).expect("Failed to create token");

    let err = get_claims_validate_jwt_token(&expired_token, audience, JWT_SECRET)
        .expect_err("Token should be expired");
    assert_eq!(
        *err.kind(),
        jsonwebtoken::errors::ErrorKind::ExpiredSignature
    );

    let www_authenticate: std::result::Result<WwwAuthenticate, Option<WwwAuthenticate>> =
        Error::BadAuthToken {
            error: err,
            location: location!(),
        }
        .try_into();
    let www_authenticate = www_authenticate.expect("Failed to convert to WwwAuthenticate");
    assert_eq!(
        www_authenticate.to_string(),
        "Bearer realm=\"api-auth\", error=\"Bad authentication token. ExpiredSignature\", kind=\"ExpiredSignature\"",
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_jwt_token_valid() {
    let username = DEMO_USER;
    let audience = "localhost";
    let claims = jwt_claims(username, audience, Duration::seconds(10));
    let expired_token = create_jwt(&claims, JWT_SECRET).expect("Failed to create token");

    let _ = get_claims_validate_jwt_token(&expired_token, audience, JWT_SECRET)
        .expect("Token should be valid");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_account_ok() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    let (_, login_resp) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");

    let access_token = login_resp.access_token;

    let (_, account_response) = http_req_with_headers::<AccountResponse>(
        &client,
        Method::GET,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/auth/account"),
        String::new().to_string(),
    )
    .await
    .expect("Failed to get account");

    assert_eq!(account_response.username, DEMO_USER);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_account_unauthorized() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(),
        DEMO_USER.to_string(),
        DEMO_PASSWORD.to_string(),
    )
    .await;
    let client = reqwest::Client::new();

    // skip login

    // do account request
    let account_err = http_req_with_headers::<()>(
        &client,
        Method::GET,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/ui/auth/account"),
        String::new().to_string(),
    )
    .await
    .expect_err("Account should fail");

    assert_eq!(account_err.status, StatusCode::UNAUTHORIZED);
}
