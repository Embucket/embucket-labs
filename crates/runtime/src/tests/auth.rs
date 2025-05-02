#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::error::ErrorResponse;
use crate::http::ui::tests::common::req;
use crate::tests::run_test_server;
use http::Method;
use reqwest;
use serde_json::json;
use std::net::SocketAddr;
use crate::http::auth::models::{LoginPayload, LoginResponse};


#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_login() {
    let addr = run_test_server().await;
    let client = reqwest::Client::new();

    let http_response = req(
        &client,
        Method::POST,
        &format!("http://{addr}/login"),
        json!(LoginPayload {
            username: String::from("admin"),
            password: String::from("admin"),
        }).to_string(),
    ).await
    .expect("Failed to login");

    let refresh_token = http_response.headers().get("Set-Cookie")
        .expect("Failed to get Set-Cookie header");
    eprintln!("Refresh token: {:#?}", refresh_token);

    let login_response = http_response.json::<LoginResponse>()
        .await
        .expect("Failed to parse login response");

    // let login_response = http_req::<LoginResponse>(
    //     &client,
    //     Method::POST,
    //     &format!("http://{addr}/login"),
    //     json!(LoginPayload {
    //         username: String::from("admin"),
    //         password: String::from("admin"),
    //     }).to_string(),
    // )
    // .await
    // .expect("Failed to login");
    eprintln!("Login response: {:#?}", login_response);
    eprintln!("Login response: {:#?}", login_response.access_token);
    assert!(false);

}