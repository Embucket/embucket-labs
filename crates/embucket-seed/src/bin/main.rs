use clap::Parser;
use std::{net::SocketAddr, str::FromStr};

use embucket_seed::seed_client::seed_database;
use embucket_seed::static_seed_assets::SeedVariant;

#[tokio::main]
#[allow(clippy::expect_used)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into())
                .add_directive("hyper=off".parse().expect("Invalid directive")),
        )
        .init();

    let opts = CliOpts::parse();

    seed_database(
        opts.server_address(),
        opts.seed_variant(),
        opts.auth_user(),
        opts.auth_password(),
    )
    .await;
}

#[derive(Parser)]
#[command(version, about, long_about=None)]
pub struct CliOpts {
    #[arg(
        short,
        long,
        value_enum,
        env = "SEED_VARIANT",
        default_value = "typical",
        help = "Variant of seed to use"
    )]
    seed_variant: SeedVariant,

    #[arg(
        long,
        env = "SERVER_ADDRESS",
        required = true,
        default_value = "http://127.0.0.1:3000",
        help = "ip:port of embucket server"
    )]
    pub server_address: String,

    #[arg(long, env = "AUTH_USER", help = "User for auth")]
    pub auth_user: String,

    #[arg(long, env = "AUTH_PASSWORD", help = "Password for auth")]
    pub auth_password: String,
}

impl CliOpts {
    #[must_use]
    pub const fn seed_variant(&self) -> SeedVariant {
        self.seed_variant
    }

    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn server_address(&self) -> SocketAddr {
        SocketAddr::from_str(&self.server_address).expect("Invalid address")
    }

    #[must_use]
    pub fn auth_user(&self) -> String {
        self.auth_user.clone()
    }

    #[must_use]
    pub fn auth_password(&self) -> String {
        self.auth_password.clone()
    }
}
