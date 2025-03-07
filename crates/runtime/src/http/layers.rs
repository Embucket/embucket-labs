// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![allow(dead_code)]
use axum::http::HeaderMap;
use axum::{middleware::Next, response::Response};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderValue, Method};
use std::str::FromStr;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use super::error;

#[derive(Clone)]
struct RequestMetadata {
    request_id: Uuid,
    auth_details: AuthDetails,
}

#[derive(Clone)]
enum AuthDetails {
    Unauthenticated,
    Authenticated { user_id: String },
}

#[allow(clippy::unwrap_used)]
pub async fn add_request_metadata(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = headers.get("x-request-id").map_or_else(Uuid::now_v7, |hv| {
        hv.to_str()
            .map(Uuid::from_str)
            .ok()
            .transpose()
            .ok()
            .flatten()
            .unwrap_or_else(Uuid::now_v7)
    });
    request.extensions_mut().insert(RequestMetadata {
        request_id,
        auth_details: AuthDetails::Unauthenticated,
    });
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert("x-request-id", request_id.to_string().parse().unwrap());
    response
}

#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub fn make_cors_middleware(origin: &str) -> Result<CorsLayer, error::RuntimeHttpError> {
    #[allow(clippy::expect_fun_call)]
    let origin_value = origin
        .parse::<HeaderValue>()
        .expect(&format!("Failed to parse origin value: {origin}"));
    Ok(CorsLayer::new()
        .allow_origin(origin_value)
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::HEAD,
        ])
        .allow_headers(vec![AUTHORIZATION, CONTENT_TYPE])
        .allow_credentials(true))
}
