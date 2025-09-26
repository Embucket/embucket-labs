pub mod error;

use snafu::ResultExt;
use std::net::SocketAddr;

use crate::external_models;
use crate::requests::rest_api_client::{RestApiClient, RestClient};
use crate::seed_generator::parse_seed_template;
use crate::seed_models::Volume;
use crate::static_seed_assets::SeedVariant;
use error::{LoadSeedSnafu, RequestSnafu, SeedResult};

/// A client for seeding database with initial data.
///
/// The `SeedClient` provides functionality to load seed templates, authenticate with the server,
/// and populate the database with seed data including volumes, databases, schemas, and tables.
pub struct SeedClient {
    /// Seed data to be loaded into the database
    pub seed_data: Vec<Volume>,
    /// REST API client used for communication with the server
    pub client: Box<dyn RestApiClient + Send>,
}

impl SeedClient {
    /// Creates a new instance of `SeedClient` with the specified server address.
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address of the Embucket service
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            seed_data: vec![],
            client: Box::new(RestClient::new(addr)),
        }
    }

    /// Loads and parses a seed template from the specified variant.
    ///
    /// # Arguments
    ///
    /// * `seed_variant` - The seed variant to load
    ///
    /// # Errors
    ///
    /// Returns `SeedError` if the seed template cannot be parsed
    pub fn try_load_seed_template(&mut self, seed_variant: SeedVariant) -> SeedResult<()> {
        let raw_seed_data = parse_seed_template(seed_variant.seed_data()).context(LoadSeedSnafu)?;
        self.seed_data = raw_seed_data.generate();
        Ok(())
    }

    /// Authenticates with the server using the provided credentials.
    ///
    /// # Arguments
    ///
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Errors
    ///
    /// Returns `SeedError` if authentication fails
    pub async fn login(&mut self, username: &str, password: &str) -> SeedResult<()> {
        self.client
            .login(username, password)
            .await
            .context(RequestSnafu)?;
        Ok(())
    }

    /// Seeds the database with all loaded data.
    ///
    /// This method processes the loaded seed data and creates all defined entities
    /// (volumes, databases, schemas, and tables) in the target database.
    ///
    /// # Returns
    ///
    /// The number of successfully seeded entities on success
    ///
    /// # Errors
    ///
    /// Returns `SeedError` if any database operation fails
    #[allow(clippy::cognitive_complexity)]
    pub async fn seed_all(&mut self) -> SeedResult<usize> {
        let mut seeded_entities: usize = 0;
        for seed_volume in &self.seed_data {
            let volume: external_models::VolumeCreatePayload = seed_volume.clone().into();
            self.client
                .create_volume(volume)
                .await
                .context(RequestSnafu)?;
            tracing::debug!(volume = %seed_volume.volume_name, "Created volume");
            seeded_entities += 1;

            for seed_database in &seed_volume.databases {
                self.client
                    .create_database(&seed_volume.volume_name, &seed_database.database_name)
                    .await
                    .context(RequestSnafu)?;
                tracing::debug!(database = %seed_database.database_name, "Created database");
                seeded_entities += 1;

                for seed_schema in &seed_database.schemas {
                    self.client
                        .create_schema(&seed_database.database_name, &seed_schema.schema_name)
                        .await
                        .context(RequestSnafu)?;
                    tracing::debug!(schema = %seed_schema.schema_name, "Created schema");
                    seeded_entities += 1;

                    for seed_table in &seed_schema.tables {
                        let table_columns: Vec<(String, String)> = seed_table
                            .columns
                            .iter()
                            .map(|col| (col.col_name.clone(), format!("{}", col.col_type)))
                            .collect();

                        self.client
                            .create_table(
                                &seed_database.database_name,
                                &seed_schema.schema_name,
                                &seed_table.table_name,
                                table_columns.as_slice(),
                            )
                            .await
                            .context(RequestSnafu)?;
                        tracing::debug!(table = %seed_table.table_name, "Created table");
                        seeded_entities += 1;
                    }
                }
            }
        }
        Ok(seeded_entities)
    }
}

/// A convenience function to perform database seeding with a single call.
///
/// This function creates a `SeedClient`, loads the specified seed template,
/// authenticates with the server, and executes the seeding process.
///
/// # Arguments
///
/// * `addr` - Socket address of the target server
/// * `seed_variant` - The seed variant to use for populating the database
/// * `user` - Username for server authentication
/// * `pass` - Password for server authentication
#[allow(clippy::cognitive_complexity)]
pub async fn seed_database(
    addr: SocketAddr,
    seed_variant: SeedVariant,
    user: String,
    pass: String,
) {
    let mut seed_client = SeedClient::new(addr);

    tracing::info!(?seed_variant, "Preparing seed data variant");

    if let Err(err) = seed_client.try_load_seed_template(seed_variant) {
        tracing::warn!(error = %err, "Seed client failed to load seed template");
        return;
    }

    if let Err(err) = seed_client.login(&user, &pass).await {
        tracing::warn!(error = %err, "Seed client failed to login on server");
        return;
    }

    tracing::info!("Seeding started!");

    match seed_client.seed_all().await {
        Ok(seeded_entities_count) => {
            tracing::info!(seeded_entities_count, "Seeding finished");
        }
        Err(err) => tracing::error!(error = %err, "Seeding error"),
    }
}
