pub(crate) mod cli;

use clap::Parser;
use dotenv::dotenv;
use embucket_runtime::{
    config::{AuthConfig, DbConfig, RuntimeConfig},
    http::config::WebConfig,
    http::web_assets::config::StaticWebConfig,
    run_binary,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]
async fn main() {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bucketd=debug,embucket_runtime=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let opts = cli::CliOpts::parse();
    let slatedb_prefix = opts.slatedb_prefix.clone();
    let host = opts.host.clone().unwrap();
    let iceberg_catalog_url = opts.catalog_url.clone().unwrap();
    let port = opts.port.unwrap();
    let web_assets_port = opts.assets_port.unwrap();
    let allow_origin = if opts.cors_enabled.unwrap_or(false) {
        opts.cors_allow_origin.clone()
    } else {
        None
    };
    let jwt_secret = opts.jwt_secret();
    let demo_user = opts.auth_demo_user.clone().unwrap();
    let demo_password = opts.auth_demo_password.clone().unwrap();

    let dbt_serialization_format = opts
        .data_format
        .clone()
        .unwrap_or_else(|| "json".to_string());
    let object_store = opts.object_store_backend();
    let mut auth_config = AuthConfig::new(jwt_secret);
    auth_config.with_demo_credentials(demo_user, demo_password);

    match object_store {
        Err(e) => {
            tracing::error!("Failed to create object store: {:?}", e);
            return;
        }
        Ok(object_store) => {
            tracing::info!("Starting embucket");

            let runtime_config = RuntimeConfig {
                db: DbConfig {
                    slatedb_prefix: slatedb_prefix.clone(),
                },
                web: WebConfig {
                    host: host.clone(),
                    port,
                    allow_origin: allow_origin.clone(),
                    data_format: dbt_serialization_format,
                    iceberg_catalog_url,
                },
                web_assets: StaticWebConfig {
                    host,
                    port: web_assets_port,
                    allow_origin,
                },
            };

            if let Err(e) = run_binary(object_store, runtime_config, auth_config).await {
                tracing::error!("Error while running: {:?}", e);
            }
        }
    }
}
