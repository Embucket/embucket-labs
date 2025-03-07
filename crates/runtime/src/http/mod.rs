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

use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use http_body_util::BodyExt;
use icebucket_metastore::Metastore;
use std::sync::Arc;
use time::Duration;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, SessionManagerLayer};

use layers::make_cors_middleware;
use session::{RequestSessionMemory, RequestSessionStore};

use crate::execution::{self, service::ExecutionService};

pub mod error;

pub mod dbt;
pub mod metastore;
pub mod ui;

pub mod config;
pub mod layers;
pub mod router;
pub mod session;
pub mod state;
pub mod utils;

//#[cfg(test)]
//mod tests;

use super::http::config::IceBucketWebConfig;

pub fn make_icebucket_app(
    metastore: Arc<dyn Metastore>,
    config: &IceBucketWebConfig,
) -> Result<Router, Box<dyn std::error::Error>> {
    let execution_cfg = execution::utils::Config::new(&config.data_format)?;
    let execution_svc = Arc::new(ExecutionService::new(metastore.clone(), execution_cfg));

    let session_memory = RequestSessionMemory::default();
    let session_store = RequestSessionStore::new(session_memory, execution_svc.clone());

    tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );

    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(5 * 60)));

    // Create the application state
    let app_state = state::AppState::new(metastore, execution_svc);

    let mut app = router::create_app(app_state)
        .layer(session_layer)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(print_request_response));

    if let Some(allow_origin) = config.allow_origin.as_ref() {
        app = app.layer(make_cors_middleware(allow_origin)?);
    }

    Ok(app)
}

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_icebucket_app(
    app: Router,
    config: &IceBucketWebConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let host = config.host.clone();
    let port = config.port;
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

/// This func will wait for a signal to shutdown the service.
/// It will wait for either a Ctrl+C signal or a SIGTERM signal.
///
/// # Panics
/// If the function fails to install the signal handler, it will panic.
#[allow(
    clippy::expect_used,
    clippy::redundant_pub_crate,
    clippy::cognitive_complexity
)]
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::warn!("Ctrl+C received, starting graceful shutdown");
        },
        () = terminate => {
            tracing::warn!("SIGTERM received, starting graceful shutdown");
        },
    }

    tracing::warn!("signal received, starting graceful shutdown");
}

async fn print_request_response(
    req: Request,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (req_parts, req_body) = req.into_parts();
    let method = req_parts.method.to_string();
    let uri = req_parts.uri.to_string();
    let bytes = buffer_and_print("request", &method, &uri, req_body).await?;
    let req = Request::from_parts(req_parts, Body::from(bytes));
    let res = next.run(req).await;

    let (resp_parts, resp_body) = res.into_parts();
    let bytes = buffer_and_print("response", &method, &uri, resp_body).await?;
    let res = Response::from_parts(resp_parts, Body::from(bytes));

    Ok(res)
}

async fn buffer_and_print<B>(
    direction: &str,
    method: &String,
    uri: &String,
    body: B,
) -> Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
{
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read {direction} body: {err}"),
            ));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        // Skip upload endpoint logs as they can be large
        if !uri.contains("upload") {
            tracing::debug!("{direction} {method} {uri} body = {body}");
        }
    }

    Ok(bytes)
}
