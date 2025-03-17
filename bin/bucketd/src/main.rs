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

use std::error::Error;

use clap::Parser;
use cli::{IceBucketCli, IceBucketSubcommand};
use dotenv::dotenv;
use icebucket_runtime::{run_icebucket, run_icebucket_sql};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]
async fn main() {
    dotenv().ok();
    let opts = cli::IceBucketCli::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                if opts.subcommand.is_some() {
                    "bucketd=error,icebucket_runtime=error".into()
                } else {
                    "bucketd=debug,icebucket_runtime=debug,tower_http=debug".into()
                }
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let result = if opts.subcommand.is_some() {
        run_sql_main(opts).await
    } else {
        run_icebucket_main(opts).await
    };

    if let Err(e) = result {
        tracing::error!("Error while running IceBucket: {:?}", e);
    }
}

#[allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]
async fn run_icebucket_main(cli: IceBucketCli) -> Result<(), Box<dyn Error>> {
    let object_store = cli.object_store_backend()?;
    tracing::info!("Starting 🧊🪣 IceBucket...");
    run_icebucket(object_store, cli.as_ref().into()).await
}

#[allow(clippy::unwrap_used)]
async fn run_sql_main(cli: IceBucketCli) -> Result<(), Box<dyn Error>> {
    let object_store = cli.object_store_backend()?;
    let runtime_config = cli.as_ref().into();

    if let Some(IceBucketSubcommand::Sql {
        query, init_file, ..
    }) = cli.subcommand
    {
        let init = init_file
            .map(|path| std::fs::read_to_string(path).unwrap())
            .unwrap_or_default();
        let query = format!("{init};\n{query}");
        run_icebucket_sql(object_store, runtime_config, query).await?;
    }

    Ok(())
}
