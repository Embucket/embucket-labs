#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::error::ErrorResponse;
use crate::http::ui::tests::common::{req, http_req_with_headers};
use crate::tests::run_test_server;
use http::{header, HeaderMap, HeaderValue, Method};
use itertools::Itertools;
use reqwest;
use serde_json::json;
use std::net::SocketAddr;
use crate::http::auth::models::{LoginPayload, LoginResponse};


fn validate_set_cookie (headers: &HeaderMap, cookie_name: &str, flags: &[&str]) -> String {
    let refresh_token_cookie_value = headers
    .get("Set-Cookie")
    .expect("Failed to get Set-Cookie header")
    .to_str()
    .expect("Failed to convert Set-Cookie header to string")
    .to_string();

    for flag in flags {
        assert!(refresh_token_cookie_value.contains(flag));
    }

    let refresh_token_cookie_parts: Vec<_> = refresh_token_cookie_value
        .split(';')
        .map(String::from)
        .collect();
    let refresh_token_key_value = refresh_token_cookie_parts[0].clone();
    assert!(refresh_token_key_value.starts_with(cookie_name));
    return refresh_token_key_value.get(cookie_name.len()+1..)
        .expect("Failed to extract value from Set-Cookie")
        .to_string();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_login() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    // login
    let (headers, login_response) = http_req_with_headers::<LoginResponse>(
        &client, Method::POST, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]),
        &format!("http://{addr}/login"),
        json!(LoginPayload {
            username: String::from("admin"),
            password: String::from("admin"),
        }).to_string(),
    ).await.expect("Failed to login");

    eprintln!("Login response: {:#?}", login_response);
    eprintln!("Login response: {:#?}", login_response.access_token);

    let access_token = login_response.access_token;
    let refresh_token = validate_set_cookie(&headers, "refresh_token", &["HttpOnly", "Secure"]);

    eprintln!("Refresh token: {:#?}", refresh_token);

    // refresh
    let (headers, refresh_response) = http_req_with_headers::<LoginResponse>(
        &client, Method::GET, 
        HeaderMap::from_iter(vec![
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (header::COOKIE, HeaderValue::from_str(
                format!("access_token={access_token}; refresh_token={refresh_token}").as_str(),
            ).expect("Can't convert to HeaderValue")),
        ]),
        &format!("http://{addr}/refresh"),
        String::new(),
    ).await.expect("Refresh request failed");

    eprintln!("Refresh response: {:#?}", refresh_response);
    eprintln!("Refresh response access token: {:#?}", refresh_response.access_token);  

    let refresh_token = validate_set_cookie(&headers, "refresh_token", &["HttpOnly", "Secure"]);
    eprintln!("Refresh token: {:#?}", refresh_token);

    assert!(false);

}