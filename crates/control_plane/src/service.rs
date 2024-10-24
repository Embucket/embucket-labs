use crate::error::{Error, Result};
use crate::models::{StorageProfile, StorageProfileCreateRequest};
use crate::models::{Warehouse, WarehouseCreateRequest};
use crate::repository::{StorageProfileRepository, WarehouseRepository};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;
use datafusion::prelude::*;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use std::collections::HashMap;

#[async_trait]
pub trait ControlService: Send + Sync {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile>;
    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile>;
    async fn delete_profile(&self, id: Uuid) -> Result<()>;
    async fn list_profiles(&self) -> Result<Vec<StorageProfile>>;

    async fn create_warehouse(&self, params: &WarehouseCreateRequest) -> Result<Warehouse>;
    async fn get_warehouse(&self, id: Uuid) -> Result<Warehouse>;
    async fn delete_warehouse(&self, id: Uuid) -> Result<()>;
    async fn list_warehouses(&self) -> Result<Vec<Warehouse>>;

    // async fn create_database(&self, params: &DatabaseCreateRequest) -> Result<Database>;
    // async fn get_database(&self, id: Uuid) -> Result<Database>;
    // async fn delete_database(&self, id: Uuid) -> Result<()>;
    // async fn list_databases(&self) -> Result<Vec<Database>>;

    // async fn create_table(&self, params: &TableCreateRequest) -> Result<Table>;
    // async fn get_table(&self, id: Uuid) -> Result<Table>;
    // async fn delete_table(&self, id: Uuid) -> Result<()>;
    // async fn list_tables(&self) -> Result<Vec<Table>>;

    async fn query_table(&self, warehouse_id:&Uuid, query:&String) -> Result<()>;
}

pub struct ControlServiceImpl {
    storage_profile_repo: Arc<dyn StorageProfileRepository>,
    warehouse_repo: Arc<dyn WarehouseRepository>,
}

impl ControlServiceImpl {
    pub fn new(
        storage_profile_repo: Arc<dyn StorageProfileRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
    ) -> Self {
        Self {
            storage_profile_repo,
            warehouse_repo,
        }
    }
}

#[async_trait]
impl ControlService for ControlServiceImpl {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile> {
        let profile = params.try_into()?;
        self.storage_profile_repo.create(&profile).await?;
        Ok(profile)
    }

    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile> {
        self.storage_profile_repo.get(id).await
    }

    async fn delete_profile(&self, id: Uuid) -> Result<()> {
        let wh = self.list_warehouses().await?;
        if wh.iter().any(|wh| wh.storage_profile_id == id) {
            return Err(Error::NotEmpty("Storage profile is in use".to_string()));
        }
        self.storage_profile_repo.delete(id).await
    }

    async fn list_profiles(&self) -> Result<Vec<StorageProfile>> {
        self.storage_profile_repo.list().await
    }

    async fn create_warehouse(&self, params: &WarehouseCreateRequest) -> Result<Warehouse> {
        // TODO: Check if storage profile exists
        // - Check if its valid
        // - Generate id, update created_at and updated_at
        // - Try create Warehouse from WarehouseCreateRequest
        let wh: Warehouse = params.try_into()?;
        let _ = self.get_profile(wh.storage_profile_id).await?;
        self.warehouse_repo.create(&wh).await?;
        Ok(wh)
    }

    async fn get_warehouse(&self, id: Uuid) -> Result<Warehouse> {
        self.warehouse_repo.get(id).await
    }

    async fn delete_warehouse(&self, id: Uuid) -> Result<()> {
        // let db = self.list_databases().await?;
        // db.iter().filter(|db| db.warehouse_id == id).next().ok_or(Error::NotEmpty("Warehouse is in use".to_string()))?;
        self.warehouse_repo.delete(id).await
    }

    async fn list_warehouses(&self) -> Result<Vec<Warehouse>> {
        self.warehouse_repo.list().await
    }

    async fn query_table(&self, warehouse_id:&Uuid, query:&String) -> Result<()> {
        let config = RestCatalogConfig::builder()
            .uri("http://0.0.0.0:3000/catalog".to_string())
            .warehouse(warehouse_id.to_string())
            .props(HashMap::default())
            .build();

        let catalog = RestCatalog::new(config);
        let catalog = IcebergCatalogProvider::try_new(Arc::new(catalog))
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_catalog("catalog", Arc::new(catalog));

        let provider = ctx.catalog("catalog").unwrap();
        let schemas = provider.schema_names();
        println!("{schemas:?}");
        assert_eq!(schemas.len(), 6);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::models::{
        AwsAccessKeyCredential, CloudProvider, Credentials, StorageProfileCreateRequest,
    };
    use crate::repository::InMemoryStorageProfileRepository;
    use crate::repository::InMemoryWarehouseRepository;

    #[tokio::test]
    async fn test_create_warehouse_failed_no_storage_profile() {
        let storage_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);
        let request = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };

        let err = service.create_warehouse(&request).await.unwrap_err();
        assert!(matches!(err, Error::ErrNotFound));
    }

    #[tokio::test]
    async fn test_delete_nonempty_storage_profile() {
        let storage_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);

        let request = StorageProfileCreateRequest {
            r#type: CloudProvider::AWS,
            region: "us-west-2".to_string(),
            bucket: "my-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            }),
            sts_role_arn: None,
            endpoint: None,
        };
        let profile = service.create_profile(&request).await.unwrap();
        let request = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: profile.id,
            prefix: "test-prefix".to_string(),
        };

        let wh = service.create_warehouse(&request).await.unwrap();
        let result = service
            .delete_profile(wh.storage_profile_id)
            .await
            .unwrap_err();
        assert!(matches!(result, Error::NotEmpty(_)));
    }
}
