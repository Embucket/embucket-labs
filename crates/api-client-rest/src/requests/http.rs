#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::error::HttpRequestError;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use reqwest;
use std::fmt::Display;

#[derive(Debug)]
pub struct HttpErrorData {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap<HeaderValue>,
    pub status: StatusCode,
    pub body: String,
    pub error: HttpRequestError,
}

impl From<HttpErrorData> for HttpRequestError {
    fn from(value: HttpErrorData) -> Self {
        let HttpErrorData { error, status, .. } = value;
        HttpRequestError::HttpRequest {
            message: error.to_string(),
            status,
        }
    }
}

impl Display for HttpErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// As of minimalistic interface this doesn't support checking request/response headers
pub async fn http_req_with_headers<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    headers: HeaderMap,
    url: &str,
    payload: String,
) -> Result<(HeaderMap, T), HttpErrorData> {
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
                    Err(HttpErrorData {
                        method,
                        url: url.to_string(),
                        headers,
                        status,
                        body: text,
                        error: HttpRequestError::HttpRequest {
                            message: err.to_string(),
                            status,
                        },
                    })
                }
            }
        }
    } else {
        let error = response
            .error_for_status_ref()
            .expect_err("Expected error, http code not OK");
        // Return custom error as reqwest error has no body contents
        Err(HttpErrorData {
            method,
            url: url.to_string(),
            headers: response.headers().clone(),
            status: response.status(),
            error: HttpRequestError::HttpRequest {
                message: error.to_string(),
                status: response.status(),
            },
            body: response.text().await.expect("Failed to get response text"),
        })
    }
}

/// As of minimalistic interface this doesn't support checking request/response headers
pub async fn http_req<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    payload: String,
) -> Result<T, HttpErrorData> {
    let headers = HeaderMap::from_iter(vec![(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    )]);
    let (_, res) = http_req_with_headers(client, method, headers, url, payload).await?;
    Ok(res)
}
