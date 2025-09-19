#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::models::AbortRequestBody;
use crate::models::{ClientEnvironment, LoginRequestBody, LoginRequestData, QueryRequestBody};
use reqwest;
use reqwest::Method;
use reqwest::StatusCode;
use reqwest::header;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug)]
pub struct TestHttpError {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap<HeaderValue>,
    pub status: StatusCode,
    pub body: String,
    pub error: String,
}

/// As of minimalistic interface this doesn't support checking request/response headers
pub async fn http_req_with_headers<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    headers: HeaderMap,
    url: &String,
    payload: String,
) -> Result<(HeaderMap, T), TestHttpError> {
    let res = client
        .request(method.clone(), url)
        .headers(headers)
        .body(payload)
        .send()
        .await;

    let response = res.unwrap();
    if response.status() == StatusCode::OK {
        let headers = response.headers().clone();
        let status = response.status();
        let text = response.text().await.expect("Failed to get response text");
        if text.is_empty() {
            // If no actual type retuned we emulate unit, by "null" value in json
            Ok((
                headers,
                serde_json::from_str::<T>("null").expect("Failed to parse response"),
            ))
        } else {
            let json = serde_json::from_str::<T>(&text);
            match json {
                Ok(json) => Ok((headers, json)),
                Err(err) => {
                    // Normally we don't expect error here, and only have http related error to return
                    Err(TestHttpError {
                        method,
                        url: url.clone(),
                        headers,
                        status,
                        body: text,
                        error: err.to_string(),
                    })
                }
            }
        }
    } else {
        let error = response
            .error_for_status_ref()
            .expect_err("Expected error, http code not OK");
        // Return custom error as reqwest error has no body contents
        Err(TestHttpError {
            method,
            url: url.clone(),
            headers: response.headers().clone(),
            status: response.status(),
            body: response.text().await.expect("Failed to get response text"),
            error: format!("{error:?}"),
        })
    }
}

#[must_use]
pub fn login_url(addr: &SocketAddr) -> String {
    format!(
        "http://{addr}/session/v1/login-request?request_id=123&databaseName=embucket&schemaName=public&warehouse=embucket"
    )
}

#[must_use]
pub fn query_url(addr: &SocketAddr, request_id: Uuid) -> String {
    format!("http://{addr}/queries/v1/query-request?requestId={request_id}")
}

#[must_use]
pub fn abort_url(addr: &SocketAddr, request_id: Uuid) -> String {
    format!("http://{addr}/queries/v1/abort-request?requestId={request_id}")
}

#[must_use]
pub fn get_query_result_url(addr: &SocketAddr, query_id: &str) -> String {
    format!("http://{addr}/queries/{query_id}/result")
}

fn login_data(login: &str, passw: &str) -> LoginRequestBody {
    LoginRequestBody {
        data: LoginRequestData {
            client_app_id: String::new(),
            client_app_version: String::new(),
            svn_revision: None,
            account_name: String::new(),
            login_name: login.to_string(),
            client_environment: ClientEnvironment {
                application: String::new(),
                os: String::new(),
                os_version: String::new(),
                python_version: String::new(),
                python_runtime: String::new(),
                python_compiler: String::new(),
                ocsp_mode: String::new(),
                tracing: 0,
                login_timeout: None,
                network_timeout: None,
                socket_timeout: None,
            },
            password: passw.to_string(),
            session_parameters: HashMap::default(),
        },
    }
}

pub async fn login<T>(
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
        &login_url(addr),
        json!(login_data(username, password)).to_string(),
    )
    .await
}

pub async fn query<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &str,
    request_id: Uuid,
    query: &str,
    async_exec: bool,
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
                HeaderValue::from_str(format!("Snowflake Token=\"{access_token}\"").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &query_url(addr, request_id),
        json!(QueryRequestBody {
            sql_text: query.to_string(),
            async_exec,
        })
        .to_string(),
    )
    .await
}

pub async fn abort<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &str,
    request_id: Uuid,
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
                HeaderValue::from_str(format!("Snowflake Token=\"{access_token}\"").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &abort_url(addr, request_id),
        json!(AbortRequestBody {
            sql_text: query.to_string(),
            request_id,
        })
        .to_string(),
    )
    .await
}

pub async fn get_query_result<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &str,
    query_id: &str,
) -> std::result::Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::GET,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Snowflake Token=\"{access_token}\"").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &get_query_result_url(addr, query_id),
        String::new(),
    )
    .await
}
