#![allow(dead_code)]
use axum::http::HeaderMap;
use axum::{middleware::Next, response::Response};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderValue, Method};
use std::str::FromStr;
use tower_http::cors::{AllowOrigin, CorsLayer};
use uuid::Uuid;

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
#[tracing::instrument(
    name = "api_ui::layers::add_request_metadata",
    level = "trace",
    skip(request, next),
    fields(response_headers)
)]
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

    // Record the result as part of the current span.
    tracing::Span::current().record("response_headers", format!("{:#?}", response.headers()));
    response
}

#[allow(clippy::needless_pass_by_value, clippy::expect_used)]
pub fn make_cors_middleware(origin: &str) -> CorsLayer {
    let origins: Vec<HeaderValue> = origin
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| HeaderValue::from_str(part).expect("Failed to parse origin value"))
        .collect();

    let allow_origin = AllowOrigin::list(origins);
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::HEAD,
            Method::PUT,
            Method::PATCH,
        ])
        .allow_headers(vec![AUTHORIZATION, CONTENT_TYPE])
        .allow_credentials(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_cors_middleware() {
        for origin in [
            "https://a.example",
            "https://b.example",
            "https://c.example http://d.example, http://e.example",
        ] {
            let _layer = make_cors_middleware(origin);
        }
    }
}
