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

use super::config::StaticWebConfig;
use super::handler::WEB_ASSETS_MOUNT_PATH;
use super::handler::{root_handler, tar_handler};
use crate::http::{layers::make_cors_middleware, shutdown_signal};
use axum::{routing::get, Router};
use core::net::SocketAddr;
use tower_http::trace::TraceLayer;

#[allow(clippy::unwrap_used, clippy::as_conversions)]
pub async fn run_web_assets_server(
    config: &StaticWebConfig,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let StaticWebConfig {
        host,
        port,
        allow_origin,
    } = config;

    let mut app = Router::new()
        .route(WEB_ASSETS_MOUNT_PATH, get(root_handler))
        .route(
            format!("{WEB_ASSETS_MOUNT_PATH}{{*path}}").as_str(),
            get(tar_handler),
        )
        .layer(TraceLayer::new_for_http());

    if let Some(allow_origin) = allow_origin.as_ref() {
        app = app.layer(make_cors_middleware(allow_origin)?);
    }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    tracing::info!("Listening on http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
    });

    Ok(addr)
}
