use std::net::SocketAddr;
use snafu::ResultExt;

use super::error::{SeedResult, LoadSeedSnafu, RequestSnafu};
use crate::seed::{Volume, read_volumes_template};
use api_client_rest::{AuthenticatedClient, api::{DatabaseClient, DatabaseClientApi}};

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
    async fn create_volumes(&mut self) -> SeedResult<()>;
    // async fn create_databases(&self) -> SeedResult<()>;
    // async fn create_schemas(&self) -> SeedResult<()>;
    // async fn create_tables(&self) -> SeedResult<()>;
    // async fn populate_data(&self) -> SeedResult<()>;
}

#[async_trait::async_trait]
impl SeedApi for SeedDatabase {
    fn try_load_seed(&mut self, _seed_variant: SeedVariant) -> SeedResult<()> {
        let raw_seed_data = read_volumes_template().context(LoadSeedSnafu)?;
        self.seed_data = raw_seed_data.generate();
        Ok(())
    }

    async fn login(&mut self, username: &str, password: &str) -> SeedResult<()> {
        self.client.login(username, password).await.context(RequestSnafu)?;
        Ok(())
    }
    
    async fn create_volumes(&mut self) -> SeedResult<()> {
        for seed_volume in &self.seed_data {
            let volume: api_structs::volumes::Volume = seed_volume.clone().into();
            self.client
                .create_volume(volume)
                .await
                .context(RequestSnafu)?;
        }
        Ok(())
    }

    // async fn create_databases(&self) -> SeedResult<()> {
    //     // self.metastore.create_database(name, database)
    //     Ok(())
    // }

    // async fn create_schemas(&self) -> SeedResult<()> {
    //     Ok(())
    // }

    // async fn create_tables(&self) -> SeedResult<()> {
    //     Ok(())
    // }

    // async fn populate_data(&self) -> SeedResult<()> {
    //     Ok(())
    // }
}
