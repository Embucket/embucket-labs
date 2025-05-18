use super::http::{http_req_with_headers, HttpErrorData};
use api_structs::{auth::LoginPayload};
use snafu::ResultExt;
use super::error::{HttpRequestResult, HttpRequestError, SerializeSnafu};
use http::{HeaderMap, HeaderValue, Method, header};
use serde_json::json;
use std::net::SocketAddr;
use reqwest;

// Auth helpers

pub async fn login<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    username: &str,
    password: &str,
) -> Result<(HeaderMap, T), HttpErrorData>
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


pub async fn refresh<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    refresh_token: &str,
) -> Result<(HeaderMap, T), HttpErrorData>
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
