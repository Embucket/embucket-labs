use super::error::{SeedResult, LoadSeedSnafu, RequestSnafu};
use crate::seed::{Volume, read_super_template};
use snafu::ResultExt;
use api_client_rest::{AuthenticatedClient, api::{DatabaseClient, DatabaseClientApi}};

pub enum SeedVariant {
    Minimal,
    Typical,
    Extreme,
    Insane,
}

pub struct SeedDatabase {
    seed_data: Option<Volume>,
    client: Box<dyn DatabaseClientApi + Send>,
}

impl SeedDatabase {
    #[must_use]
    pub fn new(client: DatabaseClient) -> Self {
        Self {
            seed_data: None,
            client: Box::new(client),
        }
    }

    pub fn try_load_seed(&mut self, _seed_variant: SeedVariant) -> SeedResult<()> {
        let raw_seed_data = read_super_template().context(LoadSeedSnafu)?;
        self.seed_data = Some(raw_seed_data.materialize());
        Ok(())
    }
}
#[async_trait::async_trait]
pub trait SeedApi {
    async fn create_volumes(mut self) -> SeedResult<()>;
    // async fn create_databases(&self) -> SeedResult<()>;
    // async fn create_schemas(&self) -> SeedResult<()>;
    // async fn create_tables(&self) -> SeedResult<()>;
    // async fn populate_data(&self) -> SeedResult<()>;
}

#[async_trait::async_trait]
impl SeedApi for SeedDatabase {
    async fn create_volumes(mut self) -> SeedResult<()> {
        if let Some(seed_volume) = self.seed_data {
            let volume: api_structs::volumes::Volume = seed_volume.into();
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
