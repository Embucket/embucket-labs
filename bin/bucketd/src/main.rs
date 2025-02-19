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

pub(crate) mod cli;

use clap::Parser;
use dotenv::dotenv;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]
async fn main() {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bucketd=debug,nexus=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let opts = cli::IceBucketOpts::parse();
    let slatedb_prefix = opts.slatedb_prefix.clone();
    let host = opts.host.clone().unwrap();
    let port = opts.port.unwrap();
    let allow_origin = if opts.cors_enabled.unwrap_or(false) {
        opts.cors_allow_origin.clone()
    } else {
        None
    };
    let object_store = opts.object_store_backend();

    match object_store {
        Err(e) => {
            tracing::error!("Failed to create object store: {:?}", e);
            return;
        }
        Ok(object_store) => {
            tracing::info!("Starting 🧊🪣 IceBucket...");

            if let Err(e) =
                nexus::run_icebucket(object_store, slatedb_prefix, host, port, allow_origin).await
            {
                tracing::error!("Failed to start IceBucket: {:?}", e);
            }
        }
    }
}
