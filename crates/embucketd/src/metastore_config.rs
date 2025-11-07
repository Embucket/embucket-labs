use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use core_metastore::{Database, Metastore, Schema, SchemaIdent, Volume, VolumeIdent};
use serde::Deserialize;
use snafu::prelude::*;
use tokio::fs;

#[derive(Debug, Deserialize, Default)]
pub struct MetastoreBootstrapConfig {
    #[serde(default)]
    volumes: Vec<VolumeEntry>,
    #[serde(default)]
    databases: Vec<DatabaseEntry>,
    #[serde(default)]
    schemas: Vec<SchemaEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct VolumeEntry {
    #[serde(flatten)]
    volume: Volume,
    #[serde(default)]
    database: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct DatabaseEntry {
    ident: String,
    volume: VolumeIdent,
}

#[derive(Debug, Deserialize, Clone)]
struct SchemaEntry {
    database: String,
    schema: String,
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Failed to read metastore config {path:?}: {source}"))]
    ReadConfig {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to parse metastore config {path:?}: {source}"))]
    ParseConfig {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[snafu(display("Metastore bootstrap error: {source}"))]
    Metastore {
        source: core_metastore::error::Error,
    },
}

const DEFAULT_SCHEMA_NAME: &str = "public";

impl MetastoreBootstrapConfig {
    pub async fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).await.context(ReadConfigSnafu {
            path: path.to_path_buf(),
        })?;
        let config = serde_yaml::from_str(&contents).context(ParseConfigSnafu {
            path: path.to_path_buf(),
        })?;
        Ok(config)
    }

    pub async fn apply(&self, metastore: Arc<dyn Metastore>) -> Result<(), ConfigError> {
        for volume_entry in &self.volumes {
            self.apply_volume(volume_entry, metastore.clone()).await?;
        }

        for db in &self.databases {
            self.ensure_database(metastore.clone(), &db.ident, &db.volume)
                .await?;
        }

        for schema in &self.schemas {
            self.ensure_schema(metastore.clone(), &schema.database, &schema.schema)
                .await?;
        }

        Ok(())
    }

    async fn apply_volume(
        &self,
        entry: &VolumeEntry,
        metastore: Arc<dyn Metastore>,
    ) -> Result<(), ConfigError> {
        if metastore
            .get_volume(&entry.volume.ident)
            .await
            .context(MetastoreSnafu)?
            .is_none()
        {
            tracing::info!(
                volume = %entry.volume.ident,
                "Creating volume from metastore config"
            );
            metastore
                .create_volume(&entry.volume.ident, entry.volume.clone())
                .await
                .context(MetastoreSnafu)?;
        } else {
            tracing::debug!(
                volume = %entry.volume.ident,
                "Volume already exists, skipping config create"
            );
        }

        if let Some(database) = &entry.database {
            self.ensure_database(metastore, database, &entry.volume.ident)
                .await?;
        }

        Ok(())
    }

    async fn ensure_database(
        &self,
        metastore: Arc<dyn Metastore>,
        ident: &str,
        volume: &str,
    ) -> Result<(), ConfigError> {
        if metastore
            .get_database(&ident.to_string())
            .await
            .context(MetastoreSnafu)?
            .is_none()
        {
            tracing::info!(database = ident, volume, "Creating database from config");
            metastore
                .create_database(
                    &ident.to_string(),
                    Database {
                        ident: ident.to_string(),
                        volume: volume.to_string(),
                        properties: None,
                    },
                )
                .await
                .context(MetastoreSnafu)?;
        }
        self.ensure_schema(metastore, ident, DEFAULT_SCHEMA_NAME)
            .await?;
        Ok(())
    }

    async fn ensure_schema(
        &self,
        metastore: Arc<dyn Metastore>,
        database: &str,
        schema: &str,
    ) -> Result<(), ConfigError> {
        let schema_ident = SchemaIdent::new(database.to_string(), schema.to_string());
        if metastore
            .get_schema(&schema_ident)
            .await
            .context(MetastoreSnafu)?
            .is_none()
        {
            tracing::info!(
                schema = schema,
                database = database,
                "Creating schema from config"
            );
            metastore
                .create_schema(
                    &schema_ident,
                    Schema {
                        ident: schema_ident.clone(),
                        properties: None,
                    },
                )
                .await
                .context(MetastoreSnafu)?;
        }
        Ok(())
    }
}
