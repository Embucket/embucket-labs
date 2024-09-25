use crate::models::{StorageProfile, StorageProfileCreateRequest};
use crate::repository::StorageProfileRepository;
use uuid::Uuid;
use async_trait::async_trait;
use std::sync::Arc;

pub enum CloudProvider {
    AWS,
    AZURE,
    GCS,
}

#[async_trait]
pub trait StorageProfileService: Send + Sync {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile, &'static str>;
    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile, &'static str>;
    async fn update_profile(&self, id: Uuid, profile: StorageProfile) -> Result<(), &'static str>;
    async fn delete_profile(&self, id: Uuid) -> Result<(), &'static str>;
    async fn list_profiles(&self) -> Vec<StorageProfile>;
}

pub struct StorageProfileServiceImpl {
    repository: Arc<dyn StorageProfileRepository>,
}

impl StorageProfileServiceImpl {
    pub fn new(repository: Arc<dyn StorageProfileRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl StorageProfileService for StorageProfileServiceImpl {
    async fn create_profile(&self, params: StorageProfileCreateRequest) -> Result<StorageProfile, &'static str> {
        let profile = StorageProfile::from(params);
        self.repository.create(profile).await;
        Ok(profile)
    }

    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile, &'static str> {
        self.repository.get(id).await.ok_or("Profile not found")
    }

    async fn update_profile(&self, id: Uuid, profile: StorageProfile) -> Result<(), &'static str> {
        if self.repository.get(id).await.is_none() {
            return Err("Profile not found");
        }
        self.repository.update(id, profile).await
    }

    async fn delete_profile(&self, id: Uuid) -> Result<(), &'static str> {
        self.repository.delete(id).await
    }

    async fn list_profiles(&self) -> Vec<StorageProfile> {
        self.repository.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use chrono::Utc;
    use crate::models::{StorageProfile, CloudProvider, Credentials, AwsAccessKeyCredential};
    use crate::repository::InMemoryStorageProfileRepository;

    fn create_service() -> StorageProfileServiceImpl {
        StorageProfileServiceImpl::new(Arc::new(InMemoryStorageProfileRepository::new()))
    }

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
        let service = create_service();
        let profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(service.create_profile(profile.clone()).await.is_ok());

        // Retrieve profile
        let retrieved_profile = service.get_profile(profile_id).await;
        assert!(retrieved_profile.is_ok());
        assert_eq!(retrieved_profile.unwrap().id, profile_id);
    }

    #[tokio::test]
    async fn test_create_invalid_profile() {
        let service = create_service();
        let mut profile = create_dummy_profile();

        // Set invalid bucket name
        profile.bucket = "a".to_string();
        assert!(service.create_profile(profile).await.is_err());
    }

    #[tokio::test]
    async fn test_update_profile() {
        let service = create_service();
        let mut profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(service.create_profile(profile.clone()).await.is_ok());

        // Update profile region
        profile.region = "eu-west-1".to_string();
        assert!(service.update_profile(profile_id, profile.clone()).await.is_ok());

        // Check if the region was updated
        let updated_profile = service.get_profile(profile_id).await.unwrap();
        assert_eq!(updated_profile.region, "eu-west-1");
    }

    #[tokio::test]
    async fn test_delete_profile() {
        let service = create_service();
        let profile = create_dummy_profile();
        let profile_id = profile.id;

        // Create profile
        assert!(service.create_profile(profile.clone()).await.is_ok());

        // Delete profile
        assert!(service.delete_profile(profile_id).await.is_ok());

        // Check if the profile is deleted
        assert!(service.get_profile(profile_id).await.is_err());
    }

    #[tokio::test]
    async fn test_list_profiles() {
        let service = create_service();

        let profile1 = create_dummy_profile();
        let profile2 = create_dummy_profile();

        // Create profiles
        assert!(service.create_profile(profile1.clone()).await.is_ok());
        assert!(service.create_profile(profile2.clone()).await.is_ok());

        // List profiles
        let profiles = service.list_profiles().await;
        assert_eq!(profiles.len(), 2);
    }
}