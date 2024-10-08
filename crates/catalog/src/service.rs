use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{
    table::Table, Namespace, NamespaceIdent, TableCommit as TableCommitOld, TableCreation,
    TableIdent,
};

use crate::error::Result; // TODO: Replace this with this crate error and result
use crate::models::{Config, TableCommit};
use crate::repository::Repository;

use control_plane::models::Warehouse;

#[async_trait]
pub trait Catalog: Repository {
    async fn get_config(&self) -> Result<Config>;
}

#[derive(Clone, Debug)]
pub struct CatalogService {
    repo: Arc<dyn Repository>,
    warehouse: Warehouse,
}

impl CatalogService {
    pub fn new(inner: Arc<dyn Repository>, warehouse: Warehouse) -> Self {
        Self {
            repo: inner,
            warehouse,
        }
    }
}

#[async_trait]
impl Catalog for CatalogService {
    async fn get_config(&self) -> Result<Config> {
        Ok(Config::default())
    }
}

#[async_trait]
impl iceberg::Catalog for CatalogService {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.repo.list_namespaces(parent).await
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.repo.create_namespace(namespace, properties).await
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.repo.get_namespace(namespace).await
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.repo.namespace_exists(namespace).await
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.repo.update_namespace(namespace, properties).await
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        // - Check if the namespace is empty
        self.repo.drop_namespace(namespace).await
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.repo.list_tables(namespace).await
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let mut creation = creation;
        creation.location = Some(self.warehouse.location.clone());
        // TODO: Add checks
        // - Check if the namespace exists
        // - Check if the table doesnt exist
        // - Check if storage profile is valid (writtable)

        self.repo.create_table(namespace, creation).await

        // - Write metadata contents to metadata_location
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.repo.load_table(table).await
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.repo.drop_table(table).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.repo.table_exists(table).await
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.repo.rename_table(src, dest).await
    }

    /// Update a table to the catalog.
    async fn update_table(&self, commit: TableCommitOld) -> Result<Table> {
        self.repo.update_table(commit).await
    }
}

#[async_trait]
impl Repository for CatalogService {
    async fn update_table_ext(&self, commit: TableCommit) -> Result<Table> {
        self.repo.update_table_ext(commit).await
    }
}
