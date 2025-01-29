use crate::error::{ControlPlaneError, ControlPlaneResult};
use crate::models::{StorageProfile, Warehouse};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use utils::Db;
use utils::{Entity, Repository};

const PROFILEPREFIX: &str = "sp";
const WAREHOUSEPREFIX: &str = "wh";
const PROFILES: &str = "sp.all";
const WAREHOUSES: &str = "wh.all";

#[async_trait]
pub trait StorageProfileRepository: Send + Sync {
    async fn create(&self, params: &StorageProfile) -> ControlPlaneResult<()>;
    async fn get(&self, id: Uuid) -> ControlPlaneResult<StorageProfile>;
    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()>;
    async fn list(&self) -> ControlPlaneResult<Vec<StorageProfile>>;
}

#[async_trait]
pub trait WarehouseRepository: Send + Sync {
    async fn create(&self, params: &Warehouse) -> ControlPlaneResult<()>;
    async fn get_by_name(&self, name: &str) -> ControlPlaneResult<Warehouse>;
    async fn get(&self, id: Uuid) -> ControlPlaneResult<Warehouse>;
    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()>;
    async fn list(&self) -> ControlPlaneResult<Vec<Warehouse>>;
}

impl Entity for StorageProfile {
    fn id(&self) -> Uuid {
        self.id
    }
}

impl Entity for Warehouse {
    fn id(&self) -> Uuid {
        self.id
    }
}

pub struct StorageProfileRepositoryDb {
    db: Arc<Db>,
}

impl StorageProfileRepositoryDb {
    pub const fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

pub struct WarehouseRepositoryDb {
    db: Arc<Db>,
}

impl WarehouseRepositoryDb {
    pub const fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

impl Repository for StorageProfileRepositoryDb {
    type Entity = StorageProfile;

    fn db(&self) -> &Db {
        &self.db
    }

    fn prefix() -> &'static str {
        PROFILEPREFIX
    }

    fn collection_key() -> &'static str {
        PROFILES
    }
}

#[async_trait]
impl StorageProfileRepository for StorageProfileRepositoryDb {
    async fn create(&self, entity: &StorageProfile) -> ControlPlaneResult<()> {
        Repository::_create(self, entity).await.map_err(Into::into)
    }

    async fn get(&self, id: Uuid) -> ControlPlaneResult<StorageProfile> {
        Repository::_get(self, id).await.map_err(Into::into)
    }

    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()> {
        Repository::_delete(self, id).await.map_err(Into::into)
    }

    async fn list(&self) -> ControlPlaneResult<Vec<StorageProfile>> {
        Repository::_list(self).await.map_err(Into::into)
    }
}

impl Repository for WarehouseRepositoryDb {
    type Entity = Warehouse;

    fn db(&self) -> &Db {
        &self.db
    }

    fn prefix() -> &'static str {
        WAREHOUSEPREFIX
    }

    fn collection_key() -> &'static str {
        WAREHOUSES
    }
}

#[async_trait]
impl WarehouseRepository for WarehouseRepositoryDb {
    async fn create(&self, entity: &Warehouse) -> ControlPlaneResult<()> {
        Repository::_create(self, entity).await.map_err(Into::into)
    }

    async fn get_by_name(&self, name: &str) -> ControlPlaneResult<Warehouse> {
        let warehouses = Repository::_list(self).await?;
        warehouses.iter().find(|&wh| wh.name == name).map_or_else(
            || {
                Err(ControlPlaneError::WarehouseNameNotFound {
                    name: name.to_string(),
                })
            },
            |wh| Ok(wh.clone()),
        )
    }

    async fn get(&self, id: Uuid) -> ControlPlaneResult<Warehouse> {
        Repository::_get(self, id).await.map_err(Into::into)
    }

    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()> {
        Repository::_delete(self, id).await.map_err(Into::into)
    }

    async fn list(&self) -> ControlPlaneResult<Vec<Warehouse>> {
        Repository::_list(self).await.map_err(Into::into)
    }
}

// In-memory repository using a mutex for safe shared access
#[derive(Debug, Default)]
pub struct InMemoryStorageProfileRepository {
    profiles: Mutex<HashMap<Uuid, StorageProfile>>,
}

#[async_trait]
#[allow(clippy::unwrap_used)]
impl StorageProfileRepository for InMemoryStorageProfileRepository {
    async fn create(&self, profile: &StorageProfile) -> ControlPlaneResult<()> {
        self.profiles
            .lock()
            .unwrap()
            .insert(profile.id, profile.clone());
        Ok(())
    }

    async fn get(&self, id: Uuid) -> ControlPlaneResult<StorageProfile> {
        Ok(self
            .profiles
            .lock()
            .unwrap()
            .get(&id)
            .ok_or(ControlPlaneError::StorageProfileNotFound { id })?
            .clone())
    }

    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()> {
        self.profiles
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or(ControlPlaneError::StorageProfileNotFound { id })?;
        Ok(())
    }

    async fn list(&self) -> ControlPlaneResult<Vec<StorageProfile>> {
        Ok(self.profiles.lock().unwrap().values().cloned().collect())
    }
}

#[async_trait]
#[allow(clippy::unwrap_used)]
impl WarehouseRepository for InMemoryWarehouseRepository {
    async fn create(&self, warehouse: &Warehouse) -> ControlPlaneResult<()> {
        self.warehouses
            .lock()
            .unwrap()
            .insert(warehouse.id, warehouse.clone());
        Ok(())
    }

    async fn get_by_name(&self, name: &str) -> ControlPlaneResult<Warehouse> {
        let warehouses = self.list().await?;
        warehouses.iter().find(|&wh| wh.name == name).map_or_else(
            || {
                Err(ControlPlaneError::WarehouseNameNotFound {
                    name: name.to_string(),
                })
            },
            |wh| Ok(wh.clone()),
        )
    }

    async fn get(&self, id: Uuid) -> ControlPlaneResult<Warehouse> {
        Ok(self
            .warehouses
            .lock()
            .unwrap()
            .get(&id)
            .ok_or(ControlPlaneError::WarehouseNotFound { id })?
            .clone())
    }

    async fn delete(&self, id: Uuid) -> ControlPlaneResult<()> {
        self.warehouses
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or(ControlPlaneError::WarehouseNotFound { id })?;
        Ok(())
    }

    async fn list(&self) -> ControlPlaneResult<Vec<Warehouse>> {
        Ok(self.warehouses.lock().unwrap().values().cloned().collect())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryWarehouseRepository {
    warehouses: Mutex<HashMap<Uuid, Warehouse>>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::models::{AwsAccessKeyCredential, CloudProvider, Credentials};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::sync::Arc;

    fn create_dummy_profile() -> StorageProfile {
        StorageProfile::new(
            CloudProvider::AWS,
            "us-west-1".to_string(),
            "bucket".to_string(),
            Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "access_key".to_string(),
                aws_secret_access_key: "secret_key".to_string(),
            }),
            None,
            None,
        )
        .expect("failed to create profile")
    }

    #[tokio::test]
    async fn test_storage_profile_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        );

        let repo = StorageProfileRepositoryDb::new(Arc::new(db));

        let profile_1 = create_dummy_profile();
        repo.create(&profile_1)
            .await
            .expect("failed to create profile");
        let profile_2 = create_dummy_profile();
        repo.create(&profile_2)
            .await
            .expect("failed to create profile");

        let profiles = repo.list().await.expect("failed to list profiles");

        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].id, profile_1.id);
        assert_eq!(profiles[1].id, profile_2.id);

        repo.delete(profile_1.id)
            .await
            .expect("failed to delete profile");

        let profiles = repo.list().await.expect("failed to list profiles");
        assert_eq!(profiles.len(), 1);
    }

    #[tokio::test]
    async fn test_warehouse_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        );

        let repo = WarehouseRepositoryDb::new(Arc::new(db));

        let wh1 = Warehouse::new("prefix".to_string(), "wh1".to_string(), Uuid::new_v4())
            .expect("failed to create warehouse");
        repo.create(&wh1).await.expect("failed to create warehouse");
        let wh2 = Warehouse::new("prefix".to_string(), "wh2".to_string(), Uuid::new_v4())
            .expect("failed to create warehouse");
        repo.create(&wh2).await.expect("failed to create warehouse");

        let warehouses = repo.list().await.expect("failed to list warehouses");

        assert_eq!(warehouses.len(), 2);
        assert_eq!(warehouses[0].id, wh1.id);
        assert_eq!(warehouses[1].id, wh2.id);

        repo.delete(wh1.id)
            .await
            .expect("failed to delete warehouse");

        let warehouses = repo.list().await.expect("failed to list warehouses");
        assert_eq!(warehouses.len(), 1);
    }
}
