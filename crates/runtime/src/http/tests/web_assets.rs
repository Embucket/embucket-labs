use crate::http::web_assets::{config::StaticWebConfig, run_web_assets_server};
use http::Method;
use reqwest;
use reqwest::header;

#[allow(clippy::expect_used)]
#[tokio::test]
async fn test_web_assets_server() {
    let addr = run_web_assets_server(&StaticWebConfig {
        host: "0.0.0.0".to_string(),
        port: 0,
        allow_origin: None,
    })
    .await;

    assert!(addr.is_ok());

    let client = reqwest::Client::new();
    let addr = addr.expect("Failed to run web assets server");
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
async fn test_web_assets_server_redirect() {
    let addr = run_web_assets_server(&StaticWebConfig {
        host: "0.0.0.0".to_string(),
        port: 0,
        allow_origin: None,
    })
    .await;

    assert!(addr.is_ok());

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Failed to build client for redirect");

    let addr = addr.expect("Failed to run web assets server");
    let res = client
        .request(Method::GET, format!("http://{addr}/deadbeaf"))
        .send()
        .await
        .expect("Failed to send request to web assets server");

    assert_eq!(http::StatusCode::SEE_OTHER, res.status());

    let redirect = res
        .headers()
        .get(header::LOCATION)
        .expect("Location header not found")
        .to_str()
        .expect("Failed to get str from Location header");
    assert_eq!(redirect, "/index.html");

    // redirect from root to index.html
    let res = client
        .request(Method::GET, format!("http://{addr}/"))
        .send()
        .await
        .expect("Failed to send request to web assets server");

    assert_eq!(http::StatusCode::SEE_OTHER, res.status());

    let redirect = res
        .headers()
        .get(header::LOCATION)
        .expect("Location header not found")
        .to_str()
        .expect("Failed to get str from Location header");
    assert_eq!(redirect, "/index.html");
}
