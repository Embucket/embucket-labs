#![allow(clippy::unwrap_used, clippy::expect_used)]

use reqwest;
use crate::databases::models::DatabaseCreatePayload;
use crate::volumes::models::VolumeCreatePayload;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use serde_json::json;
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};


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

/// As of minimalistic interface this doesn't support checking request/response headers
pub async fn http_req<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    url: &String,
    payload: String,
) -> Result<T, TestHttpError> {
    let headers = HeaderMap::from_iter(vec![(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    )]);
    let (_, res) = http_req_with_headers(client, method, headers, url, payload).await?;
    Ok(res)
}

