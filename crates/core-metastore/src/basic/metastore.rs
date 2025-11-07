use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    Metastore,
    basic::config::BasicMetastoreConfig,
    error::{self as metastore_error, Result},
    models::{
        RwObject,
        database::{Database, DatabaseIdent},
        schema::{Schema, SchemaIdent},
        table::{Table, TableCreateRequest, TableIdent, TableUpdate},
        volumes::{Volume, VolumeIdent},
    },
};
use async_trait::async_trait;
use core_utils::scan_iterator::VecScanIterator;
use dashmap::DashMap;
use object_store::ObjectStore;

/// Basic metastore implementation that reads volumes and databases from config
/// and returns "Not yet implemented" for write operations and schema/table operations
#[derive(Debug)]
pub struct BasicMetastore {
    volumes: HashMap<VolumeIdent, RwObject<Volume>>,
    databases: HashMap<DatabaseIdent, RwObject<Database>>,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
}

impl BasicMetastore {
    /// Create a new `BasicMetastore` from configuration
    #[must_use]
    pub fn new(config: BasicMetastoreConfig) -> Self {
        let mut volumes = HashMap::new();
        let mut databases = HashMap::new();

        for volume_config in config.volumes {
            let volume = volume_config.to_volume();
            let volume_ident = volume.ident.clone();

            // Add volume
            volumes.insert(volume_ident.clone(), RwObject::new(volume));

            // Add databases for this volume
            for db_config in volume_config.databases {
                let database = db_config.to_database(&volume_ident);
                let db_ident = database.ident.clone();
                databases.insert(db_ident, RwObject::new(database));
            }
        }

        Self {
            volumes,
            databases,
            object_store_cache: DashMap::new(),
        }
    }

    /// Create from YAML file
    pub fn from_yaml_file(path: &str) -> Result<Self> {
        let config = BasicMetastoreConfig::from_yaml_file(path).map_err(|e| {
            metastore_error::NotYetImplementedSnafu {
                operation: format!("Failed to load config: {e}"),
            }
            .build()
        })?;
        Ok(Self::new(config))
    }

    /// Create from YAML string
    pub fn from_yaml_str(yaml: &str) -> Result<Self> {
        let config = BasicMetastoreConfig::from_yaml_str(yaml).map_err(|e| {
            metastore_error::NotYetImplementedSnafu {
                operation: format!("Failed to parse config: {e}"),
            }
            .build()
        })?;
        Ok(Self::new(config))
    }

    fn not_implemented(operation: &str) -> Result<()> {
        metastore_error::NotYetImplementedSnafu {
            operation: operation.to_string(),
        }
        .fail()
    }
}

#[async_trait]
impl Metastore for BasicMetastore {
    // Volume operations - read-only
    fn iter_volumes(&self) -> VecScanIterator<RwObject<Volume>> {
        VecScanIterator::from_vec(self.volumes.values().cloned().collect())
    }

    async fn create_volume(
        &self,
        _name: &VolumeIdent,
        _volume: Volume,
    ) -> Result<RwObject<Volume>> {
        Self::not_implemented("create_volume - use config to define volumes")?;
        unreachable!()
    }

    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        Ok(self.volumes.get(name).cloned())
    }

    async fn update_volume(
        &self,
        _name: &VolumeIdent,
        _volume: Volume,
    ) -> Result<RwObject<Volume>> {
        Self::not_implemented("update_volume - use config to define volumes")?;
        unreachable!()
    }

    async fn delete_volume(&self, _name: &VolumeIdent, _cascade: bool) -> Result<()> {
        Self::not_implemented("delete_volume - use config to define volumes")
    }

    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        // Check cache first
        if let Some(store) = self.object_store_cache.get(name) {
            return Ok(Some(store.clone()));
        }

        // Get volume and create object store
        if let Some(volume_obj) = self.volumes.get(name) {
            let store = volume_obj.data.get_object_store()?;

            // Cache it
            self.object_store_cache.insert(name.clone(), store.clone());

            Ok(Some(store))
        } else {
            Ok(None)
        }
    }

    // Database operations - read-only
    fn iter_databases(&self) -> VecScanIterator<RwObject<Database>> {
        VecScanIterator::from_vec(self.databases.values().cloned().collect())
    }

    async fn create_database(
        &self,
        _name: &DatabaseIdent,
        _database: Database,
    ) -> Result<RwObject<Database>> {
        Self::not_implemented("create_database - use config to define databases")?;
        unreachable!()
    }

    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        Ok(self.databases.get(name).cloned())
    }

    async fn update_database(
        &self,
        _name: &DatabaseIdent,
        _database: Database,
    ) -> Result<RwObject<Database>> {
        Self::not_implemented("update_database - use config to define databases")?;
        unreachable!()
    }

    async fn delete_database(&self, _name: &DatabaseIdent, _cascade: bool) -> Result<()> {
        Self::not_implemented("delete_database - use config to define databases")
    }

    // Schema operations - not implemented
    fn iter_schemas(&self, _database: &DatabaseIdent) -> VecScanIterator<RwObject<Schema>> {
        VecScanIterator::from_vec(vec![])
    }

    async fn create_schema(
        &self,
        _ident: &SchemaIdent,
        _schema: Schema,
    ) -> Result<RwObject<Schema>> {
        Self::not_implemented("create_schema")?;
        unreachable!()
    }

    async fn get_schema(&self, _ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        Ok(None)
    }

    async fn update_schema(
        &self,
        _ident: &SchemaIdent,
        _schema: Schema,
    ) -> Result<RwObject<Schema>> {
        Self::not_implemented("update_schema")?;
        unreachable!()
    }

    async fn delete_schema(&self, _ident: &SchemaIdent, _cascade: bool) -> Result<()> {
        Self::not_implemented("delete_schema")
    }

    // Table operations - not implemented
    fn iter_tables(&self, _schema: &SchemaIdent) -> VecScanIterator<RwObject<Table>> {
        VecScanIterator::from_vec(vec![])
    }

    async fn create_table(
        &self,
        _ident: &TableIdent,
        _table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        Self::not_implemented("create_table")?;
        unreachable!()
    }

    async fn get_table(&self, _ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        Ok(None)
    }

    async fn update_table(
        &self,
        _ident: &TableIdent,
        _update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        Self::not_implemented("update_table")?;
        unreachable!()
    }

    async fn delete_table(&self, _ident: &TableIdent, _cascade: bool) -> Result<()> {
        Self::not_implemented("delete_table")
    }

    async fn table_object_store(
        &self,
        _ident: &TableIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        Ok(None)
    }

    // Helper methods
    async fn table_exists(&self, _ident: &TableIdent) -> Result<bool> {
        Ok(false)
    }

    async fn url_for_table(&self, _ident: &TableIdent) -> Result<String> {
        Self::not_implemented("url_for_table")?;
        unreachable!()
    }

    async fn volume_for_table(&self, _ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_metastore_from_config() {
        let yaml = r#"
volumes:
  - name: my_volume
    type: s3_tables
    arn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    endpoint: "https://s3tables.us-east-1.amazonaws.com"
    credentials: !AccessKey
      aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
      aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    databases:
      - name: my_db
      - name: another_db
"#;

        let metastore = BasicMetastore::from_yaml_str(yaml).expect("Failed to create metastore");

        // Test volumes
        let volumes: Vec<_> = metastore.iter_volumes().collect();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].data.ident.0, "my_volume");

        // Test get_volume
        let volume = metastore
            .get_volume(&VolumeIdent("my_volume".to_string()))
            .await
            .expect("Failed to get volume");
        assert!(volume.is_some());

        // Test databases
        let databases: Vec<_> = metastore.iter_databases().collect();
        assert_eq!(databases.len(), 2);

        // Test get_database
        let db = metastore
            .get_database(&"my_db".to_string())
            .await
            .expect("Failed to get database");
        assert!(db.is_some());
        assert_eq!(db.unwrap().data.ident, "my_db");

        // Test write operations return not implemented
        let result = metastore
            .create_volume(
                &VolumeIdent("test".to_string()),
                Volume {
                    ident: VolumeIdent("test".to_string()),
                    volume: crate::models::VolumeType::Memory,
                },
            )
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Not yet implemented")
        );
    }
}
