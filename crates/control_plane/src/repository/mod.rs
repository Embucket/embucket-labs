// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::collections::HashSet;
    use std::sync::Arc;

    async fn create_slate_db() -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        )
    }

    async fn profiles_test_repos() -> (Vec<Arc<dyn StorageProfileRepository>>, StorageProfile) {
        let db = create_slate_db().await;
        let repos: Vec<Arc<dyn StorageProfileRepository>> = vec![
            Arc::new(StorageProfileRepositoryDb::new(Arc::new(db))),
            Arc::new(InMemoryStorageProfileRepository::default()),
        ];
        (repos, StorageProfile::default())
    }

    async fn warehouses_test_repos() -> (Vec<Arc<dyn WarehouseRepository>>, Warehouse) {
        let db = create_slate_db().await;
        let repos: Vec<Arc<dyn WarehouseRepository>> = vec![
            Arc::new(WarehouseRepositoryDb::new(Arc::new(db))),
            Arc::new(InMemoryWarehouseRepository::default()),
        ];
        let wh = Warehouse::new("prefix".to_string(), "wh1".to_string(), Uuid::new_v4())
            .expect("failed to create warehouse");
        (repos, wh)
    }

    #[tokio::test]
    async fn test_storage_profile_repository() {
        let db = Arc::new(create_slate_db().await);
        let repo = StorageProfileRepositoryDb::new(db.clone());
        assert_eq!(Arc::as_ptr(&repo.db), Arc::as_ptr(&db));
        assert_eq!(StorageProfileRepositoryDb::prefix(), PROFILEPREFIX);
        assert_eq!(StorageProfileRepositoryDb::collection_key(), PROFILES);
    }

    #[tokio::test]
    async fn test_create_storage_profile() {
        let (repos, profile) = profiles_test_repos().await;
        for repo in repos {
            let result = repo.create(&profile).await;
            assert!(result.is_ok());

            let list = repo.list().await.expect("failed to list profiles");
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].id, profile.id);
        }
    }

    #[tokio::test]
    async fn test_get_storage_profile() {
        let (repos, profile) = profiles_test_repos().await;
        for repo in repos {
            repo.create(&profile)
                .await
                .expect("failed to create profile");

            let fetched_profile = repo.get(profile.id).await.expect("failed to get profile");
            assert_eq!(fetched_profile.id, profile.id);
        }
    }

    #[tokio::test]
    async fn test_delete_storage_profile() {
        let (repos, profile) = profiles_test_repos().await;
        for repo in repos {
            repo.create(&profile)
                .await
                .expect("failed to create profile");

            let result = repo.delete(profile.id).await;
            assert!(result.is_ok());
            let result = repo.get(profile.id).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_list_storage_profiles() {
        let (repos, profile) = profiles_test_repos().await;
        let profile2 = StorageProfile::default();
        for repo in repos {
            repo.create(&profile)
                .await
                .expect("failed to create profile");
            repo.create(&profile2)
                .await
                .expect("failed to create profile");

            let profiles = repo.list().await.expect("failed to list profiles");
            let profile_ids: HashSet<Uuid> = profiles.iter().map(|p| p.id).collect();
            let expected_ids: HashSet<Uuid> = vec![profile.id, profile2.id].into_iter().collect();
            assert_eq!(profile_ids.len(), 2);
            assert_eq!(profile_ids, expected_ids);
        }
    }

    #[tokio::test]
    async fn test_warehouse_repository() {
        let db = Arc::new(create_slate_db().await);
        let repo = WarehouseRepositoryDb::new(db.clone());
        assert_eq!(Arc::as_ptr(&repo.db), Arc::as_ptr(&db));
        assert_eq!(WarehouseRepositoryDb::prefix(), WAREHOUSEPREFIX);
        assert_eq!(WarehouseRepositoryDb::collection_key(), WAREHOUSES);
    }

    #[tokio::test]
    async fn test_create_warehouse() {
        let (repos, wh) = warehouses_test_repos().await;
        for repo in repos {
            let result = repo.create(&wh).await;
            assert!(result.is_ok());

            let list = repo.list().await.expect("failed to list warehouses");
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].id, wh.id);
            assert_eq!(list[0].prefix, wh.prefix);
        }
    }

    #[tokio::test]
    async fn test_get_warehouse() {
        let (repos, wh) = warehouses_test_repos().await;
        for repo in repos {
            repo.create(&wh).await.expect("failed to create warehouse");

            let fetched_profile = repo.get(wh.id).await.expect("failed to get warehouse");
            assert_eq!(fetched_profile.id, wh.id);
            assert_eq!(fetched_profile.prefix, wh.prefix);
        }
    }

    #[tokio::test]
    async fn test_delete_warehouse() {
        let (repos, wh) = warehouses_test_repos().await;
        for repo in repos {
            repo.create(&wh).await.expect("failed to create warehouse");

            let result = repo.delete(wh.id).await;
            assert!(result.is_ok());
            let result = repo.get(wh.id).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_list_warehouses() {
        let (repos, wh) = warehouses_test_repos().await;
        let wh2 = Warehouse::new("prefix2".to_string(), "wh2".to_string(), Uuid::new_v4())
            .expect("failed to create warehouse");
        for repo in repos {
            repo.create(&wh).await.expect("failed to create warehouse");
            repo.create(&wh2).await.expect("failed to create warehouse");

            let warehouses = repo.list().await.expect("failed to list profiles");
            let warehouse_ids: HashSet<Uuid> = warehouses.iter().map(|p| p.id).collect();
            let expected_ids: HashSet<Uuid> = vec![wh.id, wh2.id].into_iter().collect();
            assert_eq!(warehouse_ids.len(), 2);
            assert_eq!(warehouse_ids, expected_ids);
        }
    }
}
