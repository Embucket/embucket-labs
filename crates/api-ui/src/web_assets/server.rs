use super::handler::WEB_ASSETS_MOUNT_PATH;
use super::handler::{root_handler, tar_handler};
use crate::config::StaticWebConfig;
use axum::{Router, routing::get};
use core::net::SocketAddr;
use tower_http::trace::TraceLayer;

// TODO: Refactor this: move wiring and serve logic to embucketd
// This layer should not bother with wiring and serve logic
#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_web_assets_server(
    config: &StaticWebConfig,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let StaticWebConfig { host, port } = config;

    let app = Router::new()
        .route(WEB_ASSETS_MOUNT_PATH, get(root_handler))
        .route(
            format!("{WEB_ASSETS_MOUNT_PATH}{{*path}}").as_str(),
            get(tar_handler),
        )
        .layer(TraceLayer::new_for_http());

    // TODO: CORS settings are now handled by embucketd
    // if let Some(allow_origin) = allow_origin.as_ref() {
    // app = app.layer(make_cors_middleware(allow_origin));
    // }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    tracing::info!("Listening on http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app)
            // .with_graceful_shutdown(shutdown_signal())
            .await
    });

    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_assets::config::StaticWebConfig;
    use http::Method;
    use reqwest;
    use reqwest::header;

    #[allow(clippy::expect_used)]
    #[tokio::test]
    async fn test_web_assets_server() {
        let addr = run_web_assets_server(&StaticWebConfig {
            host: "0.0.0.0".to_string(),
            port: 0,
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
}
