use std::collections::HashMap;
use uuid::Uuid;
use crate::models::{StorageProfile, StorageProfileCreateRequest};
use async_trait::async_trait; // Required for async traits
use std::sync::Mutex;


// Define the trait with async methods
#[async_trait]
pub trait StorageProfileRepository: Send + Sync {
    async fn create(&self, profile: &StorageProfileCreateRequest) -> Result<StorageProfile, &'static str>;
    async fn get(&self, id: Uuid) -> Option<&StorageProfile>;
    async fn update(&self, id: Uuid, updated_profile: &StorageProfile) -> Result<(), &'static str>;
    async fn delete(&self, id: Uuid) -> Result<(), &'static str>;
    async fn list(&self) -> Vec<StorageProfile>;
}

// In-memory repository using a mutex for safe shared access
pub struct InMemoryStorageProfileRepository {
    profiles: Mutex<HashMap<Uuid, StorageProfile>>,
}

impl InMemoryStorageProfileRepository {
    pub fn new() -> Self {
        Self {
            profiles: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StorageProfileRepository for InMemoryStorageProfileRepository {
    async fn create(&self, profile: StorageProfile) -> Result<(), &'static str> {
        let mut profiles = self.profiles.lock().unwrap();
        if profiles.contains_key(&profile.id) {
            return Err("Profile with this ID already exists");
        }
        profiles.insert(profile.id, profile);
        Ok(())
    }

    async fn get(&self, id: Uuid) -> Option<StorageProfile> {
        let profiles = self.profiles.lock().unwrap();
        profiles.get(&id).cloned()
    }

    async fn update(&self, id: Uuid, updated_profile: StorageProfile) -> Result<(), &'static str> {
        let mut profiles = self.profiles.lock().unwrap();
        if !profiles.contains_key(&id) {
            return Err("Profile not found");
        }
        profiles.insert(id, updated_profile);
        Ok(())
    }

    async fn delete(&self, id: Uuid) -> Result<(), &'static str> {
        let mut profiles = self.profiles.lock().unwrap();
        if profiles.remove(&id).is_none() {
            return Err("Profile not found");
        }
        Ok(())
    }

    async fn list(&self) -> Vec<StorageProfile> {
        let profiles = self.profiles.lock().unwrap();
        profiles.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use chrono::Utc;
    use crate::models::{StorageProfile, CloudProvider, Credentials, AwsAccessKeyCredential};

    fn create_dummy_profile() -> StorageProfile {
        StorageProfile {
            id: Uuid::new_v4(),
            cloud_provider: CloudProvider::Aws,
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "access_key_id".to_string(),
                aws_secret_access_key: "secret_access_key".to_string(),
            }),
            sts_role_arn: None,
            endpoint: None,
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
        }
    }

    #[tokio::test]
    async fn test_create_and_get_profile() {
        let repo = InMemoryStorageProfileRepository::new();
        let profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(repo.create(profile.clone()).await.is_ok());

        // Retrieve profile by ID
        let retrieved_profile = repo.get(profile_id).await;
        assert!(retrieved_profile.is_some());
        assert_eq!(retrieved_profile.unwrap().id, profile_id);
    }

    #[tokio::test]
    async fn test_update_profile() {
        let repo = InMemoryStorageProfileRepository::new();
        let mut profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(repo.create(profile.clone()).await.is_ok());

        // Update the profile region
        profile.region = "eu-west-1".to_string();
        assert!(repo.update(profile_id, profile.clone()).await.is_ok());

        // Check that the update was successful
        let updated_profile = repo.get(profile_id).await.unwrap();
        assert_eq!(updated_profile.region, "eu-west-1");
    }

    #[tokio::test]
    async fn test_delete_profile() {
        let repo = InMemoryStorageProfileRepository::new();
        let profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(repo.create(profile.clone()).await.is_ok());

        // Delete the profile
        assert!(repo.delete(profile_id).await.is_ok());

        // Try to retrieve the deleted profile
        assert!(repo.get(profile_id).await.is_none());
    }

    #[tokio::test]
    async fn test_list_profiles() {
        let repo = InMemoryStorageProfileRepository::new();

        let profile1 = create_dummy_profile();
        let profile2 = create_dummy_profile();

        // Create profiles
        assert!(repo.create(profile1.clone()).await.is_ok());
        assert!(repo.create(profile2.clone()).await.is_ok());

        // List profiles
        let profiles = repo.list().await;
        assert_eq!(profiles.len(), 2);
    }
}