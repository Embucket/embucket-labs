use std::collections::HashMap;
use uuid::Uuid;
use crate::models::{StorageProfile, StorageProfileCreateRequest};
use crate::models::{Warehouse, WarehouseCreateRequest};
use async_trait::async_trait; // Required for async traits
use std::sync::Mutex;
use crate::error::{Result, Error};


// Define the trait with async methods
#[async_trait]
pub trait StorageProfileRepository: Send + Sync {
    async fn create(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile>;
    async fn get(&self, id: Uuid) -> Result<StorageProfile>;
    async fn delete(&self, id: Uuid) -> Result<()>;
    async fn list(&self) -> Result<Vec<StorageProfile>>;
}

#[async_trait]
pub trait WarehouseRepository: Send + Sync {
    async fn create(&self, params: &WarehouseCreateRequest) -> Result<Warehouse>;
    async fn get(&self, id: Uuid) -> Result<Warehouse>;
    async fn delete(&self, id: Uuid) -> Result<()>;
    async fn list(&self) -> Result<Vec<Warehouse>>;
}

// In-memory repository using a mutex for safe shared access
#[derive(Debug, Default)]
pub struct InMemoryStorageProfileRepository {
    profiles: Mutex<HashMap<Uuid, StorageProfile>>,
}

#[derive(Debug, Default)]
pub struct InMemoryWarehouseRepository {
    warehouses: Mutex<HashMap<Uuid, Warehouse>>,
}


#[async_trait]
impl StorageProfileRepository for InMemoryStorageProfileRepository {
    async fn create(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile> {
        let mut profiles = self.profiles.lock().unwrap();
        // Ideally, we would validate the input here (or earlier in the stack)
        // Ideally, we couldn't create invalid profiles, perhaps by using a builder pattern
        // or with TryFrom/TryInto traits
        let profile = StorageProfile::try_from(params)?;
        profiles.insert(profile.id, profile.clone());
        Ok(profile)
    }

    async fn get(&self, id: Uuid) -> Result<StorageProfile> {
        let profiles = self.profiles.lock().unwrap();
        let profile = profiles.get(&id).ok_or(Error::ErrNotFound)?;
        Ok(profile.clone())
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        let mut profiles = self.profiles.lock().unwrap();
        profiles.remove(&id).ok_or(Error::ErrNotFound)?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<StorageProfile>> {
        let profiles = self.profiles.lock().unwrap();
        Ok(profiles.values().cloned().collect())
    }
}

#[async_trait]
impl WarehouseRepository for InMemoryWarehouseRepository {
    async fn create(&self, params: &WarehouseCreateRequest) -> Result<Warehouse> {
        let mut warehouses = self.warehouses.lock().unwrap();
        let warehouse = Warehouse::try_from(params)?;
        warehouses.insert(warehouse.id, warehouse.clone());
        Ok(warehouse)
    }

    async fn get(&self, id: Uuid) -> Result<Warehouse> {
        let warehouses = self.warehouses.lock().unwrap();
        let warehouse = warehouses.get(&id).ok_or(Error::ErrNotFound)?;
        Ok(warehouse.clone())
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        let mut warehouses = self.warehouses.lock().unwrap();
        warehouses.remove(&id).ok_or(Error::ErrNotFound)?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<Warehouse>> {
        let warehouses = self.warehouses.lock().unwrap();
        Ok(warehouses.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CloudProvider, Credentials, AwsAccessKeyCredential};

    fn create_dummy_profile() -> StorageProfileCreateRequest {
        StorageProfileCreateRequest {
            cloud_provider: CloudProvider::AWS,
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "access_key_id".to_string(),
                aws_secret_access_key: "secret_access_key".to_string(),
            }),
            sts_role_arn: None,
            endpoint: None,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_profile() {
        let repo = InMemoryStorageProfileRepository::default();
        let profile = create_dummy_profile();
        let profile = repo.create(&profile).await.unwrap();
        repo.get(profile.id).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_profile() {
        let repo = InMemoryStorageProfileRepository::default();
        let profile = create_dummy_profile();
        let profile = repo.create(&profile).await.unwrap();
        let id = profile.id;
        repo.delete(profile.id).await.unwrap();

        assert_eq!(repo.get(id).await.unwrap_err(), Error::ErrNotFound);
    }

    #[tokio::test]
    async fn test_list_profiles() {
        let repo = InMemoryStorageProfileRepository::default();

        let profile1 = create_dummy_profile();
        let profile2 = create_dummy_profile();

        // Create profiles
        repo.create(&profile1).await.unwrap();
        repo.create(&profile2).await.unwrap();

        // List profiles
        let profiles = repo.list().await.unwrap();
        assert_eq!(profiles.len(), 2);
    }

    #[tokio::test]
    async fn test_create_and_get_warehouse() {
        let repo = InMemoryWarehouseRepository::default();
        let warehouse = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };
        let warehouse = repo.create(&warehouse).await.unwrap();
        repo.get(warehouse.id).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_warehouse() {
        let repo = InMemoryWarehouseRepository::default();
        let warehouse = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };
        let warehouse = repo.create(&warehouse).await.unwrap();
        let id = warehouse.id;
        repo.delete(warehouse.id).await.unwrap();

        assert_eq!(repo.get(id).await.unwrap_err(), Error::ErrNotFound);
    }

    #[tokio::test]
    async fn test_list_warehouses() {
        let repo = InMemoryWarehouseRepository::default();

        let warehouse1 = WarehouseCreateRequest {
            name: "test-warehouse1".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };
        let warehouse2 = WarehouseCreateRequest {
            name: "test-warehouse2".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };

        // Create warehouses
        repo.create(&warehouse1).await.unwrap();
        repo.create(&warehouse2).await.unwrap();

        // List warehouses
        let warehouses = repo.list().await.unwrap();
        assert_eq!(warehouses.len(), 2);
    }
}