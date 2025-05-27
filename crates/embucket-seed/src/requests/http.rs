#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Low-level HTTP client functionality for the Embucket API.
//!
//! This module provides the core HTTP request handling used by the higher-level API clients.
//! It includes error handling, request/response serialization, and HTTP client configuration.

use super::error::HttpRequestError;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use reqwest;
use std::fmt::Display;

/// Represents detailed error information for HTTP request failures.
///
/// This struct captures all relevant information about a failed HTTP request,
/// including the request method, URL, headers, status code, response body,
/// and the underlying error.
#[allow(dead_code)]
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
        let HttpErrorData {
            error,
            status,
            body,
            ..
        } = value;
        Self::HttpRequest {
            message: format!("{error:?}, body: {body:#?}"),
            status,
        }
    }
}

impl Display for HttpErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// Sends an HTTP request and parses the JSON response.
///
/// This is a low-level function that handles the actual HTTP request/response
/// including error handling and response deserialization.
///
///
/// # Type Parameters
/// * `T` - The type to deserialize the response JSON into
///
/// # Arguments
/// * `client` - The reqwest client to use for the request
/// * `method` - The HTTP method to use (GET, POST, etc.)
/// * `headers` - The HTTP headers to include in the request
/// * `url` - The URL to send the request to
/// * `payload` - The request body as a string (typically JSON)
///
/// # Returns
/// A tuple containing the response headers and the deserialized response body
///
/// # Errors
/// Returns `HttpErrorData` if the request fails or the response cannot be parsed
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
            // If no actual type returned we emulate unit by "null" value in json
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
