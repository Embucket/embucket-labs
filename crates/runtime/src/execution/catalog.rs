use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProviderList, CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use icebucket_metastore::{IceBucketSchemaIdent, Metastore};


pub struct IceBucketDFMetastore {
    pub metastore: Arc<dyn Metastore>,
}

impl std::fmt::Debug for IceBucketDFMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFMetastore")
            .finish()
    }
}

// Explore using AsyncCatalogProviderList alongside CatalogProviderList
impl CatalogProviderList for IceBucketDFMetastore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn datafusion::catalog::CatalogProvider>,
    ) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        // This is currently a NOOP because we don't support registering new catalogs yet
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        // TODO: Cache the catalog names in the metastore to avoid async calls
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_databases()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|catalog| catalog.ident.clone())
                    .collect()
            })
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        let database = tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.get_database(&name.to_string())
                    .await
                    .unwrap_or_default()
            });
        database.map(|database| {
            Arc::new(IceBucketDFCatalog {
                ident: database.ident.clone(),
                metastore: self.metastore.clone(),
            }) as Arc<dyn datafusion::catalog::CatalogProvider>
        })
    }
}

pub struct IceBucketDFCatalog {
    pub ident: String,
    pub metastore: Arc<dyn Metastore>,
}

impl std::fmt::Debug for IceBucketDFCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFCatalog")
            .field("ident", &self.ident)
            .finish()
    }
}

impl CatalogProvider for IceBucketDFCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_schemas(&self.ident)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|schema| schema.ident.schema.clone())
                    .collect()
            })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        let schema = tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.get_schema(&IceBucketSchemaIdent {
                    database: self.ident.clone(),
                    schema: name.to_string(),
                })
                    .await
                    .unwrap_or_default()
            });
        schema.map(|schema| {
            Arc::new(IceBucketDFSchema {
                database: schema.ident.database.clone(),
                schema: schema.ident.schema.clone(),
                metastore: self.metastore.clone(),
            }) as Arc<dyn datafusion::catalog::SchemaProvider>
        })
    }
}

pub struct IceBucketDFSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
}

impl std::fmt::Debug for IceBucketDFSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for IceBucketDFSchema {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_tables(&IceBucketSchemaIdent { schema: self.schema.clone(), database: self.database.clone() })
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|table| table.ident.table.clone())
                    .collect()
            })
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = self.metastore.get_table(&IceBucketSchemaIdent { schema: self.schema.clone(), database: self.database.clone() }, name)
            .await?;
        
    }

    /// If supported by the implementation, adds a new table named `name` to
    /// this schema.
    ///
    /// If a table of the same name was already registered, returns "Table
    /// already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool;
    
}