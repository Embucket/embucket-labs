use std::any::Any;
use std::sync::{Arc};

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use datafusion::error::Result;

use crate::datafusion::data_catalog::extended_catalog::ExtendedIcebergCatalog;
use crate::datafusion::data_catalog::schema::MultiSchemaProvider;




use iceberg_rust::{
    catalog::{
        namespace::Namespace,
        Catalog,
    },
};
use std::fmt::Debug;

#[derive(Debug)]
pub struct IcebucketCatalogProvider {
    catalog: Arc<ExtendedIcebergCatalog>,
}

impl IcebucketCatalogProvider {
    pub async fn new(catalog: Arc<dyn Catalog>, branch: Option<&str>) -> Result<Self> {
        Ok(IcebucketCatalogProvider {
            catalog: Arc::new(
                ExtendedIcebergCatalog::new(catalog, branch.map(ToOwned::to_owned)).await?,
            ),
        })
    }

    pub fn new_sync(catalog: Arc<dyn Catalog>, branch: Option<&str>) -> Self {
        IcebucketCatalogProvider {
            catalog: Arc::new(ExtendedIcebergCatalog::new_sync(
                catalog,
                branch.map(ToOwned::to_owned),
            )),
        }
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog.catalog()
    }
}

impl CatalogProvider for IcebucketCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        let namespaces = self.catalog.schema_names(None);
        match namespaces {
            Err(_) => vec![],
            Ok(namespaces) => namespaces.into_iter().map(|x| x.to_string()).collect(),
        }
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(MultiSchemaProvider::new(
            Namespace::try_new(
                &name
                    .split('.')
                    .map(|z| z.to_owned())
                    .collect::<Vec<String>>(),
            )
            .ok()?,
            Arc::clone(&self.catalog),
        )) as Arc<dyn SchemaProvider>)
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use datafusion::catalog::CatalogProvider;
    use iceberg_rest_catalog::apis::configuration::Configuration;
    use iceberg_rust::catalog::commit::{CommitTable, CommitView};
    use iceberg_rust::catalog::create::CreateTable;
    use iceberg_rust::error::Error;
    use iceberg_rust::materialized_view::MaterializedView;
    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_rust::spec::identifier::FullIdentifier;
    use iceberg_rust::table::Table;
    use iceberg_rust::view::View;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::{Arc, RwLock};

    pub(crate) async fn prepare_mock_rest_catalog(warehouse_id: &str) -> Arc<dyn Catalog> {
        let fs_object_store = ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new()));
        let config = {
            let mut config = Configuration::new();
            config.base_path = "http://0.0.0.0:3000/catalog".to_string();
            config
        };
        Arc::new(MockRestCatalog::new(
            Some(warehouse_id.to_string().as_str()),
            config,
            fs_object_store,
        ))
    }

    #[derive(Debug)]
    pub struct MockRestCatalog {
        /// In‑memory storage of namespaces.
        namespaces: RwLock<HashMap<String, HashMap<String, String>>>,
        /// In‑memory storage of tables.
        /// The key is a string derived from the namespace and table name.
        tables: RwLock<HashMap<String, Table>>,
        name: Option<String>,
        configuration: Configuration,
        object_store_builder: ObjectStoreBuilder,
    }

    impl MockRestCatalog {
        pub fn new(
            name: Option<&str>,
            configuration: Configuration,
            object_store_builder: ObjectStoreBuilder,
        ) -> Self {
            Self {
                namespaces: RwLock::new(HashMap::new()),
                tables: RwLock::new(HashMap::new()),
                name: name.map(ToString::to_string),
                configuration,
                object_store_builder,
            }
        }

        fn table_key(identifier: &Identifier) -> String {
            format!(
                "{}.{}",
                identifier.namespace().to_string(),
                identifier.name()
            )
        }
    }
    #[async_trait]
    impl Catalog for MockRestCatalog {
        fn name(&self) -> &str {
            "MockCatalog"
        }

        async fn create_namespace(
            &self,
            namespace: &Namespace,
            properties: Option<HashMap<String, String>>,
        ) -> Result<HashMap<String, String>, Error> {
            let mut ns_map = self.namespaces.write().unwrap();
            let key = namespace.to_string();
            let props = properties.unwrap_or_default();
            ns_map.insert(key, props.clone());
            Ok(props)
        }

        async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), Error> {
            let mut ns_map = self.namespaces.write();
            ns_map.unwrap().remove(&namespace.to_string());
            Ok(())
        }

        async fn load_namespace(
            &self,
            namespace: &Namespace,
        ) -> Result<HashMap<String, String>, Error> {
            let ns_map = self.namespaces.write();
            ns_map
                .unwrap()
                .get(&namespace.to_string())
                .cloned()
                .ok_or_else(|| {
                    Error::NotFound(format!("Namespace {} not found", namespace.to_string()))
                })
        }

        async fn update_namespace(
            &self,
            namespace: &Namespace,
            updates: Option<HashMap<String, String>>,
            removals: Option<Vec<String>>,
        ) -> Result<(), Error> {
            let mut ns_map = self.namespaces.write().unwrap();
            let key = namespace.to_string();
            if let Some(props) = ns_map.get_mut(&key) {
                if let Some(upd) = updates {
                    for (k, v) in upd {
                        props.insert(k, v);
                    }
                }
                if let Some(removals) = removals {
                    for k in removals {
                        props.remove(&k);
                    }
                }
                Ok(())
            } else {
                Err(Error::NotFound(format!("Namespace {} not found", key)))
            }
        }

        async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, Error> {
            let ns_map = self.namespaces.write().unwrap();
            Ok(ns_map.contains_key(&namespace.to_string()))
        }

        async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, Error> {
            let tables = self.tables.write().unwrap();
            let ns_prefix = format!("{}.", namespace.to_string());
            let ids = tables
                .keys()
                .filter(|key| key.starts_with(&ns_prefix))
                .filter_map(|key| {
                    let parts: Vec<String> = key.split('.').map(|s| s.to_owned()).collect();
                    Identifier::try_new(&parts, None).ok()
                })
                .collect();
            Ok(ids)
        }

        async fn list_namespaces(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, Error> {
            let ns_map = self.namespaces.write().unwrap();
            let namespaces = ns_map
                .keys()
                .map(|s| Namespace::try_new(&[s.clone()]).unwrap())
                .collect();
            Ok(namespaces)
        }

        async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, Error> {
            let tables = self.tables.write().unwrap();
            Ok(tables.contains_key(&Self::table_key(identifier)))
        }

        async fn drop_table(&self, identifier: &Identifier) -> Result<(), Error> {
            let mut tables = self.tables.write().unwrap();
            tables.remove(&Self::table_key(identifier));
            Ok(())
        }

        async fn drop_view(&self, identifier: &Identifier) -> Result<(), Error> {
            self.drop_table(identifier).await
        }

        async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), Error> {
            self.drop_table(identifier).await
        }

        async fn load_tabular(self: Arc<Self>, _identifier: &Identifier) -> Result<Tabular, Error> {
            Err(Error::NotSupported(
                "load_tabular not implemented in MockCatalog".into(),
            ))
        }

        async fn create_table(
            self: Arc<Self>,
            _identifier: Identifier,
            _create_table: CreateTable,
        ) -> Result<Table, Error> {
            Err(Error::NotSupported(
                "create_table not implemented in MockCatalog".into(),
            ))
        }

        async fn create_view(
            self: Arc<Self>,
            _identifier: Identifier,
            _create_view: CreateView<Option<()>>,
        ) -> Result<View, Error> {
            Err(Error::NotSupported(
                "create_view not implemented in MockCatalog".into(),
            ))
        }

        async fn create_materialized_view(
            self: Arc<Self>,
            _identifier: Identifier,
            _create_view: CreateMaterializedView,
        ) -> Result<MaterializedView, Error> {
            Err(Error::NotSupported(
                "create_materialized_view not implemented in MockCatalog".into(),
            ))
        }

        async fn update_table(self: Arc<Self>, _commit: CommitTable) -> Result<Table, Error> {
            Err(Error::NotSupported(
                "update_table not implemented in MockCatalog".into(),
            ))
        }

        async fn update_view(
            self: Arc<Self>,
            _commit: CommitView<Option<()>>,
        ) -> Result<View, Error> {
            Err(Error::NotSupported(
                "update_view not implemented in MockCatalog".into(),
            ))
        }

        async fn update_materialized_view(
            self: Arc<Self>,
            _commit: CommitView<FullIdentifier>,
        ) -> Result<MaterializedView, Error> {
            Err(Error::NotSupported(
                "update_materialized_view not implemented in MockCatalog".into(),
            ))
        }

        async fn register_table(
            self: Arc<Self>,
            identifier: Identifier,
            _metadata_location: &str,
        ) -> Result<Table, Error> {
            let key = Self::table_key(&identifier);
            let table = Table::new(identifier, self.clone(), TableMetadata::default()).await?;
            let mut tables = self.tables.write().unwrap();
            tables
                .insert(key.clone(), table)
                .ok_or_else(|| Error::External(format!("Table {} already exists", key).into()))
        }

        fn object_store(&self, _bucket: Bucket) -> Arc<dyn ObjectStore> {
            // Return a dummy in-memory object store.
            Arc::new(InMemory::new())
        }
    }

    pub(crate) const TEST_WAREHOUSE_ID: &str = "test_warehouse_id";

    #[tokio::test]
    async fn test_icebucket_catalog_provider_new() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let provider = IcebucketCatalogProvider::new(catalog, None).await;
        assert!(provider.is_ok());
    }

    #[tokio::test]
    async fn test_icebucket_catalog_provider_new_sync() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let provider = IcebucketCatalogProvider::new_sync(catalog, None);
        assert!(Arc::strong_count(&provider.catalog) > 0);
    }

    #[tokio::test]
    async fn test_schema() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let provider = IcebucketCatalogProvider::new_sync(catalog, None);
        let schema = provider.schema("db.schema");
        assert!(schema.is_some());
    }

    #[tokio::test]
    async fn test_register_schema_unimplemented() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let provider = IcebucketCatalogProvider::new_sync(catalog, None);
        let namespace = Namespace::try_new(&["default".to_string()]).unwrap();
        let schema = Arc::new(MultiSchemaProvider::new(
            namespace,
            Arc::clone(&provider.catalog),
        ));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            provider.register_schema("test", schema)
        }))
        .is_err());
    }
}
