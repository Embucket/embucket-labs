use crate::models::{StorageProfile, StorageProfileCreateRequest};
use crate::repository::StorageProfileRepository;
use uuid::Uuid;
use async_trait::async_trait;
use std::sync::Arc;
use crate::error::{Result, Error};



#[async_trait]
pub trait StorageProfileService: Send + Sync {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile>;
    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile>;
    async fn delete_profile(&self, id: Uuid) -> Result<()>;
    async fn list_profiles(&self) -> Result<Vec<StorageProfile>>;
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
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile> {
        self.repository.create(params).await
    }

    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile> {
        self.repository.get(id).await
    }

    async fn delete_profile(&self, id: Uuid) -> Result<()> {
        self.repository.delete(id).await
    }

    async fn list_profiles(&self) -> Result<Vec<StorageProfile>> {
        self.repository.list().await
    }
}
