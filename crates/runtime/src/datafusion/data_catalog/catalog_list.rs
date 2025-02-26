use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion_common::Result;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::RwLock;
use std::{any::Any, sync::Arc};

#[derive(Debug)]
pub struct IcebucketCatalogProviderList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl IcebucketCatalogProviderList {
    pub fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }

    pub fn from_existing(catalog_list: Arc<dyn CatalogProviderList>) -> Result<Self> {
        let catalogs = catalog_list.catalog_names();
        let mut map = HashMap::new();

        for name in catalogs {
            if let Some(catalog) = catalog_list.catalog(&name) {
                map.insert(name, catalog);
            }
        }

        Ok(Self {
            catalogs: RwLock::new(map),
        })
    }
}

impl CatalogProviderList for IcebucketCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.write().unwrap();
        catalogs.insert(name, catalog.clone());
        Some(catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::data_catalog::catalog::tests::{
        prepare_mock_rest_catalog, TEST_WAREHOUSE_ID,
    };
    use crate::datafusion::data_catalog::catalog::IcebucketCatalogProvider;
    use crate::datafusion::data_catalog::extended_catalog::ExtendedIcebergCatalog;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemoryCatalogProviderList};
    use datafusion_iceberg::catalog::catalog::IcebergCatalog;
    use std::sync::Arc;

    #[test]
    fn test_icebucket_catalog_provider_list_new() {
        let catalog_list = IcebucketCatalogProviderList::new();
        assert!(catalog_list.catalog_names().is_empty());
    }

    #[tokio::test]
    async fn test_from_existing() {
        let existing_list = Arc::new(MemoryCatalogProviderList::new());
        existing_list.register_catalog("test".to_string(), Arc::new(MemoryCatalogProvider::new()));
        let catalog_list = IcebucketCatalogProviderList::from_existing(existing_list);
        assert!(catalog_list.is_ok());
        assert!(catalog_list
            .unwrap()
            .catalogs
            .read()
            .unwrap()
            .contains_key("test"));
    }

    #[tokio::test]
    async fn test_register_catalog() {
        let catalog_list = IcebucketCatalogProviderList::new();
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let extended_catalog = IcebucketCatalogProvider::new(catalog, None).await.unwrap();
        let result =
            catalog_list.register_catalog("testIcebucket".to_string(), Arc::new(extended_catalog));
        assert!(result.is_some());
        assert_eq!(catalog_list.catalog_names(), vec!["testIcebucket"]);
    }

    #[tokio::test]
    async fn test_catalog_names() {
        let catalog_list = IcebucketCatalogProviderList::new();
        let rest_catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let extended_catalog1 = IcebucketCatalogProvider::new(rest_catalog.clone(), None)
            .await
            .unwrap();
        let extended_catalog2 = IcebucketCatalogProvider::new(rest_catalog, None)
            .await
            .unwrap();

        catalog_list.register_catalog("test1".to_string(), Arc::new(extended_catalog1));
        catalog_list.register_catalog("test2".to_string(), Arc::new(extended_catalog2));

        let names = catalog_list.catalog_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"test1".to_string()));
        assert!(names.contains(&"test2".to_string()));
    }

    #[tokio::test]
    async fn test_catalog() {
        let catalog_list = IcebucketCatalogProviderList::new();
        let rest_catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let extended_catalog1 = IcebucketCatalogProvider::new(rest_catalog.clone(), None)
            .await
            .unwrap();
        catalog_list.register_catalog("test".to_string(), Arc::new(extended_catalog1));

        let result = catalog_list.catalog("test");
        assert!(result.is_some());

        let not_found = catalog_list.catalog("nonexistent");
        assert!(not_found.is_none());
    }
}
