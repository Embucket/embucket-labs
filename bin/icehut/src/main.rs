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
                .unwrap_or_else(|_| "icehut=debug,nexus=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let opts = cli::IceHutOpts::parse();
    let slatedb_prefix = opts.slatedb_prefix.clone();
    let host = opts.host.clone().unwrap();
    let port = opts.port.unwrap();
    let object_store = opts.object_store_backend();

    match object_store {
        Err(e) => {
            tracing::error!("Failed to create object store: {:?}", e);
            return;
        }
        Ok(object_store) => {
            tracing::info!("Starting ❄️🏠 IceHut...");

            if let Err(e) = nexus::run_icehut(object_store, slatedb_prefix, host, port).await {
                tracing::error!("Failed to start IceHut: {:?}", e);
            }
        }
    }
}
