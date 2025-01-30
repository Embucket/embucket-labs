use dashmap::DashMap;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::{datasource::TableProvider, error::DataFusionError};
use datafusion_iceberg::catalog::mirror::Mirror;
use futures::{executor::LocalPool, task::LocalSpawnExt};
use iceberg_rust::object_store::store::IcebergStore;
use iceberg_rust::spec::{tabular::TabularMetadata, view_metadata::REF_PREFIX};
use iceberg_rust::{
    catalog::{
        create::{CreateMaterializedView, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog,
    },
    error::Error as IcebergError,
    object_store::Bucket,
    spec::table_metadata::new_metadata_location,
};
use std::{collections::HashSet, sync::Arc};

type NamespaceNode = HashSet<String>;

#[derive(Debug)]
enum Node {
    Namespace(NamespaceNode),
    Relation(Identifier),
}

#[derive(Debug)]
pub struct ExtendedMirror {
    pub base: Arc<Mirror>,
    pub memory_catalog_provider: MemoryCatalogProvider,
}

impl ExtendedMirror {
    /// Creates a new `ExtendedMirror` with support for both Iceberg and memory tables
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        branch: Option<String>,
    ) -> Result<Self, DataFusionError> {
        let base_mirror = Mirror::new(catalog.clone(), branch).await?;
        Ok(Self {
            base: Arc::new(base_mirror),
            memory_catalog_provider: MemoryCatalogProvider::new(),
        })
    }

    pub fn new_sync(catalog: Arc<dyn Catalog>, branch: Option<String>) -> Self {
        let base_mirror = Mirror::new_sync(catalog.clone(), branch);

        Self {
            base: Arc::new(base_mirror),
            memory_catalog_provider: MemoryCatalogProvider::new(),
        }
    }

    /// Lists all tables in the given namespace.
    pub fn table_names(&self, namespace: &Namespace) -> Result<Vec<Identifier>, DataFusionError> {
        let mut table_names = self.base.table_names(namespace)?;
        let memory_tables: Vec<Identifier> = match self
            .memory_catalog_provider
            .schema(namespace.to_string().as_str())
        {
            Some(schema) => schema
                .table_names()
                .into_iter()
                .filter_map(|name| Identifier::try_new(&[name], None).ok())
                .collect(),
            None => vec![],
        };
        table_names.extend(memory_tables);
        Ok(table_names)
    }

    /// Lists all namespaces in the catalog.
    pub fn schema_names(&self, _parent: Option<&str>) -> Result<Vec<Namespace>, DataFusionError> {
        self.base.schema_names(_parent)
    }

    pub async fn table(
        &self,
        identifier: Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let iceberg_table = self.base.table(identifier.clone()).await?;
        if iceberg_table.is_some() {
            return Ok(iceberg_table);
        }
        if let Some(schema) = self
            .memory_catalog_provider
            .schema(identifier.namespace().to_string().as_str())
        {
            schema.table(identifier.name()).await
        } else {
            Err(DataFusionError::Plan("Schema does not exist".to_string()))
        }
    }

    pub fn table_exists(&self, identifier: Identifier) -> bool {
        self.base.table_exists(identifier.clone())
            || self
                .memory_catalog_provider
                .schema(identifier.namespace().to_string().as_str())
                .unwrap()
                .table_exist(identifier.name())
    }

    pub fn register_table(
        &self,
        identifier: Identifier,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match table.as_any().is::<MemTable>() {
            true => self
                .memory_catalog_provider
                .schema(identifier.namespace().to_string().as_str())
                .unwrap()
                .register_table(identifier.name().to_string(), table),
            false => self.base.register_table(identifier.clone(), table.clone()),
        }
    }
    pub fn deregister_table(
        &self,
        identifier: Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self.base.deregister_table(identifier.clone()) {
            Ok(table) => Ok(table),
            Err(_) => self
                .memory_catalog_provider
                .schema(identifier.namespace().to_string().as_str())
                .unwrap()
                .deregister_table(identifier.name()),
        }
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.base.catalog()
    }
}
