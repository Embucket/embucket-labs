use crate::web_assets::web_assets_app;
use core::net::SocketAddr;
use http::Method;
use reqwest;
use reqwest::header;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_test_web_assets_server() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let app = web_assets_app();
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 0))).await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move { axum::serve(listener, app).await });

    Ok(addr)
}

#[allow(clippy::expect_used)]
#[tokio::test]
async fn test_web_assets_server() {
    let addr = run_test_web_assets_server()
        .await
        .expect("Failed to run web assets server");

    let client = reqwest::Client::new();
    let res = client
        .request(Method::GET, format!("http://{addr}/index.html"))
        .send()
        .await
        .expect("Failed to send request to web assets server");

    assert_eq!(http::StatusCode::OK, res.status());

    let content_length = res
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("Content-Length header not found")
        .to_str()
        .expect("Failed to get str from Content-Length header")
        .parse::<i64>()
        .expect("Failed to parse Content-Length header");

    assert!(content_length > 0);
}

#[allow(clippy::expect_used)]
#[tokio::test]
async fn test_web_assets_server_spa_and_root_fallback() {
    let addr = run_test_web_assets_server()
        .await
        .expect("Failed to run web assets server");

    let client = reqwest::Client::new();

    // SPA fallback route (e.g., /test)
    let res = client
        .request(Method::GET, format!("http://{addr}/test"))
        .send()
        .await
        .expect("Failed to send request to web assets server");

    assert_eq!(http::StatusCode::OK, res.status());

    // There should be no Location header
    assert!(res.headers().get(header::LOCATION).is_none());

    // The content type should be for index.html
    let content_type = res
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("Content-Type header not found")
        .to_str()
        .expect("Failed to get str from Content-Type header");
    assert!(
        content_type.starts_with("text/html"),
        "Content-Type was {content_type}, expected text/html"
    );

    // Root path "/"
    let res_root = client
        .request(Method::GET, format!("http://{addr}/"))
        .send()
        .await
        .expect("Failed to send request to web assets server");

    assert_eq!(http::StatusCode::OK, res_root.status());

    // There should be no Location header
    assert!(res_root.headers().get(header::LOCATION).is_none());

    // The content type should also be for index.html
    let content_type_root = res_root
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("Content-Type header not found")
        .to_str()
        .expect("Failed to get str from Content-Type header");
    assert!(
        content_type_root.starts_with("text/html"),
        "Content-Type was {content_type_root}, expected text/html"
    );
}