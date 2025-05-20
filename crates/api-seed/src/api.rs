use snafu::ResultExt;
use std::net::SocketAddr;

use super::error::{LoadSeedSnafu, RequestSnafu, SeedResult};
use crate::seed::Volume;
use crate::static_assets::read_seed_template;
use api_client_rest::api::{DatabaseClient, DatabaseClientApi};
use clap::ValueEnum;

#[derive(Copy, Clone, ValueEnum)]
pub enum SeedVariant {
    Minimal,
    Typical,
    Extreme,
    Insane,
}

pub struct SeedDatabase {
    pub seed_data: Vec<Volume>,
    pub client: Box<dyn DatabaseClientApi + Send>,
}

impl SeedDatabase {
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            seed_data: vec![],
            client: Box::new(DatabaseClient::new(addr)),
        }
    }
}
#[async_trait::async_trait]
pub trait SeedApi {
    fn try_load_seed(&mut self, _seed_variant: SeedVariant) -> SeedResult<()>;

    async fn login(&mut self, username: &str, password: &str) -> SeedResult<()>;
    async fn seed_all(&mut self) -> SeedResult<()>;
    // async fn populate_data(&mut self) -> SeedResult<()>;
}

#[async_trait::async_trait]
impl SeedApi for SeedDatabase {
    fn try_load_seed(&mut self, seed_variant: SeedVariant) -> SeedResult<()> {
        let raw_seed_data = read_seed_template(seed_variant).context(LoadSeedSnafu)?;
        self.seed_data = raw_seed_data.generate();
        Ok(())
    }

    async fn login(&mut self, username: &str, password: &str) -> SeedResult<()> {
        self.client
            .login(username, password)
            .await
            .context(RequestSnafu)?;
        Ok(())
    }

    async fn seed_all(&mut self) -> SeedResult<()> {
        for seed_volume in &self.seed_data {
            let volume: api_structs::volumes::Volume = seed_volume.clone().into();
            self.client
                .create_volume(volume)
                .await
                .context(RequestSnafu)?;

            for seed_database in &seed_volume.databases {
                self.client
                    .create_database(&seed_volume.volume_name, &seed_database.database_name)
                    .await
                    .context(RequestSnafu)?;

                for seed_schema in &seed_database.schemas {
                    self.client
                        .create_schema(&seed_database.database_name, &seed_schema.schema_name)
                        .await
                        .context(RequestSnafu)?;
                }
            }
        }
        Ok(())
    }

    // async fn populate_data(&self) -> SeedResult<()> {
    //     Ok(())
    // }
}
