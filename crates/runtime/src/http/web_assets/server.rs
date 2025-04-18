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

use axum::{routing::get, Router};

use crate::http::layers::make_cors_middleware;

use super::config::StaticWebConfig;
use super::handler::WEB_ASSETS_MOUNT_PATH;
use super::handler::tar_handler;
use crate::http::shutdown_signal;
use tower_http::trace::TraceLayer;

pub async fn run_web_assets_server(config: &StaticWebConfig) -> Result<(), Box<dyn std::error::Error + Send>> {
    let StaticWebConfig { host, port, allow_origin } = config;

    let mut app = Router::new()
        .route(format!("{WEB_ASSETS_MOUNT_PATH}{{*path}}").as_str(), get(tar_handler))
        .layer(TraceLayer::new_for_http());

    if let Some(allow_origin) = allow_origin.as_ref() {
        app = app
            .layer(make_cors_middleware(allow_origin)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?);
    }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

    tracing::info!("Listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
}
