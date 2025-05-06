#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::http::auth::models::{AuthResponse, LoginPayload};
use crate::http::ui::queries::models::{QueryCreatePayload, QueryCreateResponse};
use crate::http::metastore::handlers::RwObjectVec;
use crate::http::ui::tests::common::{http_req_with_headers, TestHttpError};
use crate::tests::run_test_server_with_demo_auth;
use http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use reqwest;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use embucket_metastore::models::Volume;

const JWT_SECRET: &str = "test";
const DEMO_USER: &str = "demo_user";
const DEMO_PASSWORD: &str = "demo_password";

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

async fn login<T>(client: &reqwest::Client, addr: &SocketAddr, username: &str, password: &str) -> Result<(HeaderMap, T), TestHttpError> 
where
    T: serde::de::DeserializeOwned {
    http_req_with_headers::<T>(
        &client,
        Method::POST,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/auth/login"),
        json!(LoginPayload {
            username: String::from(username),
            password: String::from(password),
        })
        .to_string(),
    )
    .await
}

async fn logout<T>(client: &reqwest::Client, addr: &SocketAddr) -> Result<(HeaderMap, T), TestHttpError> 
where
    T: serde::de::DeserializeOwned {
    http_req_with_headers::<T>(
        &client,
        Method::POST,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/auth/logout"),
        String::new(),
    )
    .await
}

async fn refresh<T>(client: &reqwest::Client, addr: &SocketAddr, refresh_token: &str) -> Result<(HeaderMap, T), TestHttpError> 
where
    T: serde::de::DeserializeOwned {
    http_req_with_headers::<T>(
        &client,
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
        &format!("http://{addr}/auth/refresh"),
        String::new(),
    )
    .await
}

async fn query<T>(client: &reqwest::Client, addr: &SocketAddr, access_token: &String, query: &str) -> Result<(HeaderMap, T), TestHttpError> 
where
    T: serde::de::DeserializeOwned {
    http_req_with_headers::<T>(
        &client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str()).expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: None,
            query: query.to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
}

async fn metastore<T>(client: &reqwest::Client, addr: &SocketAddr, access_token: &String) -> Result<(HeaderMap, T), TestHttpError> 
where
    T: serde::de::DeserializeOwned {
    http_req_with_headers::<T>(
        &client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str()).expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/v1/metastore/volumes"),
        String::new(),
    )
    .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_login_no_secret_set() {
    // No secret set
    let addr = run_test_server_with_demo_auth(
        "".to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
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
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
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
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_metastore_request_unauthorized() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
    let client = reqwest::Client::new();

    let _ = login::<()>(&client, &addr, "", "")
        .await
        .expect_err("Login should fail");

    // Unauthorized error while running metastore request
    let metastore_err = metastore::<()>(&client, &addr, &"xyz".to_string())
        .await
        .expect_err("Metastore request should fail");
    assert_eq!(metastore_err.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_metastore_request_passes_authorization() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
    let client = reqwest::Client::new();

    let (_, login_response) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");

    // Metastore request not returning auth error
    let metastore_res = metastore::<RwObjectVec<Volume>>(&client, &addr, &login_response.access_token)
        .await;
    if let Err(e) = metastore_res {
        assert_ne!(e.status, StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_query_request_unauthorized() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
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
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
    let client = reqwest::Client::new();

    // login
    let (_, login_response) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");

    // Successfuly run query
    let (_, query_response) = query::<QueryCreateResponse>(
        &client, &addr, &login_response.access_token, "SELECT 1")
        .await
        .expect("Failed to run query");
    assert_eq!(query_response.data.query, "SELECT 1");

}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_refresh_bad_token() {
    let addr = run_test_server_with_demo_auth(
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
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
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
    let client = reqwest::Client::new();

    // login
    let (headers, _) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");
    assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

    // logout
    let (headers, _) = logout::<()>(&client, &addr)
        .await
        .expect("Failed to logout");

    // logout reset cookies to empty values
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
        JWT_SECRET.to_string(), DEMO_USER.to_string(), DEMO_PASSWORD.to_string()
    ).await;
    let client = reqwest::Client::new();

    // login
    let (headers, login_response) = login::<AuthResponse>(&client, &addr, DEMO_USER, DEMO_PASSWORD)
        .await
        .expect("Failed to login");

    eprintln!("Login response headers: {:#?}", headers);

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (refresh_token, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("HttpOnly"));
    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("Secure"));
    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("SameSite=Strict"));

    // Successfuly run query
    let (_, query_response) = query::<QueryCreateResponse>(
        &client, &addr, &login_response.access_token, "SELECT 1")
        .await
        .expect("Failed to run query");
    assert_eq!(query_response.data.query, "SELECT 1");

    //
    // test refresh handler, using refresh_token from cookie from login
    //
    let (headers, _) = refresh::<AuthResponse>(&client, &addr, refresh_token)
        .await
        .expect("Refresh request failed");

    eprintln!("Refresh response headers: {:#?}", headers);

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (_, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("HttpOnly"));
    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("Secure"));
    assert!(refresh_token_cookie
        .to_str()
        .expect("Bad cookie")
        .contains("SameSite=Strict"));
}
