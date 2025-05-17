#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::common::http_requests::{http_req_with_headers, TestHttpError};
use api_structs::{auth::LoginPayload, query::QueryCreatePayload};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use serde_json::json;
use serde::{Serialize, de::DeserializeOwned};
use std::net::SocketAddr;
use reqwest;

async fn login<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    username: &str,
    password: &str,
) -> Result<(HeaderMap, T), TestHttpError>
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


async fn refresh<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    refresh_token: &str,
) -> Result<(HeaderMap, T), TestHttpError>
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
) -> Result<(HeaderMap, T), TestHttpError>
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
            query: query.to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
}
