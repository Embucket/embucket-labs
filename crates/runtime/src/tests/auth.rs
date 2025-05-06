#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::collections::HashMap;
use crate::http::ui::tests::common::http_req_with_headers;
use crate::tests::run_test_server;
use http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use reqwest;
use serde_json::json;
use crate::http::auth::models::{LoginPayload, AuthResponse};


fn get_set_cookie_from_response_headers (headers: &HeaderMap) -> HashMap<&str, (&str, &HeaderValue)> {
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_bad_login() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let login_error = http_req_with_headers::<()>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]),
        &format!("http://{addr}/auth/login"),
        json!(LoginPayload {
            username: String::new(),
            password: String::new(),
        }).to_string(),
    ).await.expect_err("Login should fail");

    let www_auth = login_error.headers.get(header::WWW_AUTHENTICATE).expect("No WWW-Authenticate header");
    assert_eq!(www_auth.to_str().expect("Bad header encoding"), "Bearer realm=\"login\", error=\"Login error\"");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_refresh_bad_token() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let refresh_err = http_req_with_headers::<()>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (header::COOKIE, HeaderValue::from_str(
                format!("access_token=xyz; refresh_token=xyz").as_str(),
            ).expect("Can't convert to HeaderValue")),
        ]),
        &format!("http://{addr}/auth/refresh"),
        String::new(),
    ).await.expect_err("Refresh should fail");
    assert_eq!(refresh_err.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_logout() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    // login
    let (headers, _) = http_req_with_headers::<AuthResponse>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]),
        &format!("http://{addr}/auth/login"),
        json!(LoginPayload {
            username: String::from("admin"),
            password: String::from("admin"),
        }).to_string(),
    ).await.expect("Failed to login");
    assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

    // logout
    let (headers, _) = http_req_with_headers::<()>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]),
        &format!("http://{addr}/auth/logout"),
        String::new(),
    ).await.expect("Failed to logout");

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
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    // login
    let (headers, _) = http_req_with_headers::<AuthResponse>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]),
        &format!("http://{addr}/auth/login"),
        json!(LoginPayload {
            username: String::from("admin"),
            password: String::from("admin"),
        }).to_string(),
    ).await.expect("Failed to login");

    eprintln!("Login response headers: {:#?}", headers);

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (refresh_token, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("HttpOnly"));
    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("Secure"));
    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("SameSite=Strict"));

    //
    // test refresh handler, using refresh_token from cookie from login
    //
    let (headers, _) = http_req_with_headers::<AuthResponse>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (header::COOKIE, HeaderValue::from_str(
                format!("refresh_token={refresh_token}").as_str(),
            ).expect("Can't convert to HeaderValue")),
        ]),
        &format!("http://{addr}/auth/refresh"),
        String::new(),
    ).await.expect("Refresh request failed");

    eprintln!("Refresh response headers: {:#?}", headers);

    let set_cookies = get_set_cookie_from_response_headers(&headers);

    let (_, refresh_token_cookie) = set_cookies
        .get("refresh_token")
        .expect("No Set-Cookie found with refresh_token");

    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("HttpOnly"));
    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("Secure"));
    assert!(refresh_token_cookie.to_str().expect("Bad cookie").contains("SameSite=Strict"));
}
