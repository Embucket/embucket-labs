use axum::http::HeaderMap;
use axum::{middleware::Next, response::Response};
use std::str::FromStr;
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

pub async fn add_request_metadata(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id: Uuid = if let Some(hv) = headers.get("x-request-id") {
        hv.to_str()
            .map(Uuid::from_str)
            .ok()
            .transpose()
            .ok()
            .flatten()
            .unwrap_or_else(Uuid::now_v7)
    } else {
        Uuid::now_v7()
    };
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
