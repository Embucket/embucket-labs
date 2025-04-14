// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::execution::error::{self as ex_error, ExecutionResult};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider},
    datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl},
    execution::{object_store::ObjectStoreRegistry, options::ReadOptions},
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion_common::{exec_err, DataFusionError, Result as DFResult};
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use futures::executor::block_on;
use iceberg_rust::{
    catalog::{
        commit::{CommitTable as IcebergCommitTable, CommitView as IcebergCommitView},
        create::{
            CreateMaterializedView as IcebergCreateMaterializedView,
            CreateTable as IcebergCreateTable, CreateView as IcebergCreateView,
        },
        tabular::Tabular as IcebergTabular,
        Catalog as IcebergCatalog,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView as IcebergMaterializedView,
    object_store::Bucket as IcebergBucket,
    spec::identifier::Identifier as IcebergIdentifier,
    table::Table as IcebergTable,
    view::View as IcebergView,
};
use iceberg_rust_spec::{
    identifier::FullIdentifier as IcebergFullIdentifier, namespace::Namespace as IcebergNamespace,
};
use icebucket_metastore::error::MetastoreResult;
use icebucket_metastore::{
    error::MetastoreError, IceBucketSchema, IceBucketSchemaIdent, IceBucketTableCreateRequest,
    IceBucketTableIdent, IceBucketTableUpdate, Metastore,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use snafu::ResultExt;
use url::Url;

pub const DEFAULT_CATALOG: &str = "icebucket";

type TableProviderCache = DashMap<String, Arc<dyn TableProvider>>;
type SchemaProviderCache = DashMap<String, TableProviderCache>;
type CatalogProviderCache = DashMap<String, SchemaProviderCache>;
#[derive(Clone)]
pub struct IceBucketDFMetastore {
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<CatalogProviderCache>,
    pub table_object_store: Arc<DashMap<String, Arc<dyn ObjectStore>>>,
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl IceBucketDFMetastore {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        let table_object_store: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        table_object_store.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        Self {
            metastore,
            mirror: Arc::new(DashMap::new()),
            table_object_store: Arc::new(table_object_store),
            catalogs: DashMap::default(),
        }
    }

    #[allow(clippy::as_conversions, clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip(self, ctx))]
    pub async fn refresh(&self, ctx: &SessionContext) -> ExecutionResult<()> {
        let mut seen: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::default();

        let databases = self
            .metastore
            .list_databases(None, None)
            .await
            .context(ex_error::MetastoreSnafu)?;
        for database in databases {
            let db_entry = self
                .mirror
                .entry(database.ident.clone())
                .or_insert(SchemaProviderCache::default());
            let db_seen_entry = seen.entry(database.ident.clone()).or_default();
            let schemas = self
                .metastore
                .list_schemas(&database.ident, None, None)
                .await
                .context(ex_error::MetastoreSnafu)?;
            for schema in schemas {
                let schema_entry = db_entry
                    .entry(schema.ident.schema.clone())
                    .insert(TableProviderCache::default());
                let schema_seen_entry = db_seen_entry
                    .entry(schema.ident.schema.clone())
                    .or_default();
                let tables = self
                    .metastore
                    .list_tables(&schema.ident, None, None)
                    .await
                    .context(ex_error::MetastoreSnafu)?;
                for table in tables {
                    let table_url = self
                        .metastore
                        .url_for_table(&table.ident)
                        .await
                        .context(ex_error::MetastoreSnafu)?;
                    let table_object_store = self
                        .metastore
                        .table_object_store(&table.ident)
                        .await
                        .context(ex_error::MetastoreSnafu)?
                        .ok_or(MetastoreError::TableObjectStoreNotFound {
                            table: table.ident.table.clone(),
                            schema: table.ident.schema.clone(),
                            db: table.ident.database.clone(),
                        })
                        .context(ex_error::MetastoreSnafu)?;
                    let url = Url::parse(&table_url).context(ex_error::UrlParseSnafu)?;
                    self.table_object_store
                        .insert(get_url_key(&url), table_object_store.clone());

                    let table_provider = match table.format {
                        icebucket_metastore::IceBucketTableFormat::Parquet => {
                            let parq_read_options = ParquetReadOptions::default();
                            let listing_options = parq_read_options.to_listing_options(
                                ctx.state().config(),
                                ctx.state().default_table_options(),
                            );

                            let table_path = ListingTableUrl::parse(&table_url)
                                .context(ex_error::DataFusionSnafu)?;

                            // TODO: Switch Metastore to use arrow schema instead and just use that instead of scanning
                            let schema = listing_options
                                .infer_schema(&ctx.state(), &table_path)
                                .await
                                .context(ex_error::DataFusionSnafu)?;
                            let config = ListingTableConfig::new(table_path)
                                .with_listing_options(listing_options)
                                .with_schema(schema);
                            Arc::new(
                                ListingTable::try_new(config).context(ex_error::DataFusionSnafu)?,
                            ) as Arc<dyn TableProvider>
                        }
                        icebucket_metastore::IceBucketTableFormat::Iceberg => {
                            let bridge = Arc::new(IceBucketIcebergBridge {
                                metastore: self.metastore.clone(),
                                database: table.ident.clone().database,
                                object_store: table_object_store.clone(),
                            });

                            let ib_identifier = IcebergIdentifier::new(
                                &[table.ident.database.clone(), table.ident.schema.clone()],
                                &table.ident.table,
                            );
                            let tabular = bridge
                                .load_tabular(&ib_identifier)
                                .await
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                                .context(ex_error::DataFusionSnafu)?;
                            Arc::new(IcebergDataFusionTable::new(tabular, None, None, None))
                                as Arc<dyn TableProvider>
                        }
                    };
                    //mirror_entry.insert(table.ident.table.clone());
                    schema_entry.insert(table.ident.table.clone(), table_provider.clone());
                    schema_seen_entry.insert(table.ident.table.clone());
                }
            }
        }

        for db in self.mirror.iter_mut() {
            if seen.contains_key(db.key()) {
                for schema in db.iter_mut() {
                    if seen[db.key()].contains_key(schema.key()) {
                        let mut tables_to_remove = vec![];
                        for table in schema.iter() {
                            if !seen[db.key()][schema.key()].contains(table.key()) {
                                tables_to_remove.push(table.key().clone());
                            }
                        }

                        for table in tables_to_remove {
                            schema.remove(&table);
                        }
                    } else {
                        db.remove(schema.key());
                    }
                }
            } else {
                self.mirror.remove(db.key());
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for IceBucketDFMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFMetastore").finish()
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
#[must_use]
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}

impl ObjectStoreRegistry for IceBucketDFMetastore {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let url = get_url_key(url);
        self.table_object_store.insert(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion_common::Result<Arc<dyn ObjectStore>> {
        let url = get_url_key(url);
        if let Some(object_store) = self.table_object_store.get(&url) {
            Ok(object_store.clone())
        } else {
            Err(DataFusionError::Execution(format!(
                "Object store not found for url {url}"
            )))
        }
    }
}

// Explore using AsyncCatalogProviderList alongside CatalogProviderList
impl CatalogProviderList for IceBucketDFMetastore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut catalog_names = self
            .catalogs
            .iter()
            .map(|c| c.key().clone())
            .collect::<Vec<String>>();
        catalog_names.extend(self.mirror.iter().map(|e| e.key().clone()));
        catalog_names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(catalog) = self.catalogs.get(name) {
            return Some(catalog.value().clone());
        }
        if !self.mirror.contains_key(name) {
            return None;
        }
        let iceberg_catalog = IceBucketIcebergBridge::new(self.metastore.clone(), name.to_string())
            .ok()
            .map(Arc::new)?;
        let catalog: Arc<dyn CatalogProvider> = Arc::new(IceBucketDFCatalog {
            ident: name.to_string(),
            metastore: self.metastore.clone(),
            mirror: self.mirror.clone(),
            iceberg_catalog,
        });
        Some(catalog)
    }
}

pub struct IceBucketDFCatalog {
    pub ident: String,
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<CatalogProviderCache>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

impl IceBucketDFCatalog {
    #[must_use]
    pub fn catalog(&self) -> Arc<dyn IcebergCatalog> {
        self.iceberg_catalog.clone()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for IceBucketDFCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFCatalog")
            .field("ident", &self.ident)
            .finish()
    }
}

impl CatalogProvider for IceBucketDFCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self
            .mirror
            .get(&self.ident)
            .map(|db| db.iter().map(|schema| schema.key().clone()).collect())
            .unwrap_or_default();
        schemas
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(db) = self.mirror.get(&self.ident) {
            if db.contains_key(name) {
                let schema: Arc<dyn SchemaProvider> = Arc::new(IceBucketDFSchema {
                    database: self.ident.clone(),
                    schema: name.to_string(),
                    metastore: self.metastore.clone(),
                    mirror: self.mirror.clone(),
                });
                return Some(schema);
            }
        }
        None
    }
}

pub struct IceBucketDFSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<CatalogProviderCache>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for IceBucketDFSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("metastore", &"")
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
        self.mirror
            .get(&self.database)
            .and_then(|db| {
                db.get(&self.schema)
                    .map(|schema| schema.iter().map(|table| table.key().clone()).collect())
            })
            .unwrap_or_default()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(db) = self.mirror.get(&self.database) {
            if let Some(schema) = db.get(&self.schema) {
                if let Some(table) = schema.get(name) {
                    return Ok(Some(table.clone()));
                }
            }
        }
        Ok(None)
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
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.mirror
            .get(&self.database)
            .and_then(|db| db.get(&self.schema).map(|schema| schema.contains_key(name)))
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct IceBucketIcebergBridge {
    pub metastore: Arc<dyn Metastore>,
    pub database: String,
    pub object_store: Arc<dyn ObjectStore>,
}

impl IceBucketIcebergBridge {
    fn new(metastore: Arc<dyn Metastore>, database: String) -> MetastoreResult<Self> {
        let db = block_on(metastore.get_database(&database))?.ok_or(
            MetastoreError::DatabaseNotFound {
                db: database.clone(),
            },
        )?;
        let object_store = block_on(metastore.volume_object_store(&db.volume))?.ok_or(
            MetastoreError::VolumeNotFound {
                volume: db.volume.clone(),
            },
        )?;
        Ok(Self {
            metastore,
            database,
            object_store,
        })
    }
}

#[async_trait]
impl IcebergCatalog for IceBucketIcebergBridge {
    /// Name of the catalog
    fn name(&self) -> &str {
        &self.database
    }

    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &IcebergNamespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        let schema = IceBucketSchema {
            ident: schema_ident.clone(),
            properties: properties.clone(),
        };
        let schema = self
            .metastore
            .create_schema(&schema_ident, schema)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(schema.data.properties.unwrap_or_default())
    }

    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, namespace: &IcebergNamespace) -> Result<(), IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        self.metastore
            .delete_schema(&schema_ident, true)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(())
    }

    /// Load the namespace properties from the catalog
    async fn load_namespace(
        &self,
        namespace: &IcebergNamespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        let schema = self
            .metastore
            .get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match schema {
            Some(schema) => Ok(schema.data.properties.unwrap_or_default()),
            None => Err(IcebergError::NotFound(format!(
                "Namespace {}",
                namespace.join("")
            ))),
        }
    }

    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        namespace: &IcebergNamespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        let schema = self
            .metastore
            .get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match schema {
            Some(schema) => {
                let mut schema = schema.data;
                let mut properties = schema.properties.unwrap_or_default();
                if let Some(updates) = updates {
                    properties.extend(updates);
                }
                if let Some(removals) = removals {
                    for key in removals {
                        properties.remove(&key);
                    }
                }
                schema.properties = Some(properties);
                self.metastore
                    .update_schema(&schema_ident, schema)
                    .await
                    .map_err(|e| IcebergError::External(Box::new(e)))?;
                Ok(())
            }
            None => Err(IcebergError::NotFound(format!(
                "Namespace {}",
                namespace.join("")
            ))),
        }
    }

    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &IcebergNamespace) -> Result<bool, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        Ok(self
            .metastore
            .get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .is_some())
    }

    /// Lists all tables in the given namespace.
    async fn list_tabulars(
        &self,
        namespace: &IcebergNamespace,
    ) -> Result<Vec<IcebergIdentifier>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported(
                "Nested namespaces are not supported".to_string(),
            ));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.name().to_string(),
            schema: namespace.join(""),
        };
        Ok(self
            .metastore
            .list_tables(&schema_ident, None, None)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .iter()
            .map(|table| {
                IcebergIdentifier::new(
                    &[table.ident.database.clone(), table.ident.schema.clone()],
                    &table.ident.table,
                )
            })
            .collect())
    }

    /// Lists all namespaces in the catalog.
    async fn list_namespaces(
        &self,
        _parent: Option<&str>,
    ) -> Result<Vec<IcebergNamespace>, IcebergError> {
        let mut namespaces = Vec::new();
        let databases = self
            .metastore
            .list_databases(None, None)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        for database in databases {
            let schemas = self
                .metastore
                .list_schemas(&database.ident, None, None)
                .await
                .map_err(|e| IcebergError::External(Box::new(e)))?;
            for schema in schemas {
                namespaces.push(IcebergNamespace::try_new(&[
                    schema.ident.database.clone(),
                    schema.ident.schema.clone(),
                ])?);
            }
        }
        Ok(namespaces)
    }

    /// Check if a table exists
    async fn tabular_exists(&self, identifier: &IcebergIdentifier) -> Result<bool, IcebergError> {
        let table_ident = IceBucketTableIdent::from_iceberg_ident(identifier);
        Ok(self
            .metastore
            .get_table(&table_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .is_some())
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        let table_ident = IceBucketTableIdent::from_iceberg_ident(identifier);
        self.metastore
            .delete_table(&table_ident, true)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(())
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_view(&self, _identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "Views are not supported".to_string(),
        ))
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_materialized_view(
        &self,
        _identifier: &IcebergIdentifier,
    ) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "Materialized views are not supported".to_string(),
        ))
    }

    /// Load a table.
    async fn load_tabular(
        self: Arc<Self>,
        identifier: &IcebergIdentifier,
    ) -> Result<IcebergTabular, IcebergError> {
        let table_ident = IceBucketTableIdent::from_iceberg_ident(identifier);
        let table = self
            .metastore
            .get_table(&table_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match table {
            Some(table) => {
                let iceberg_table =
                    IcebergTable::new(identifier.clone(), self.clone(), table.metadata.clone())
                        .await?;

                Ok(IcebergTabular::Table(iceberg_table))
            }
            None => Err(IcebergError::NotFound(format!(
                "Table {} not found",
                identifier.name()
            ))),
        }
    }

    /// Create a table in the catalog if it doesn't exist.
    async fn create_table(
        self: Arc<Self>,
        identifier: IcebergIdentifier,
        create_table: IcebergCreateTable,
    ) -> Result<IcebergTable, IcebergError> {
        let ident = IceBucketTableIdent {
            database: self.name().to_string(),
            schema: identifier.namespace().to_string(),
            table: identifier.name().to_string(),
        };
        let table_create_request = IceBucketTableCreateRequest {
            ident: ident.clone(),
            schema: create_table.schema,
            location: create_table.location,
            partition_spec: create_table.partition_spec,
            sort_order: create_table.write_order,
            stage_create: create_table.stage_create,
            volume_ident: None,
            is_temporary: None,
            format: None,
            properties: None,
        };

        let table = self
            .metastore
            .create_table(&ident, table_create_request)
            .await
            .context(ex_error::MetastoreSnafu)
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(IcebergTable::new(identifier.clone(), self.clone(), table.metadata.clone()).await?)
    }

    /// Create a view with the catalog if it doesn't exist.
    async fn create_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: IcebergCreateView<Option<()>>,
    ) -> Result<IcebergView, IcebergError> {
        Err(IcebergError::NotSupported(
            "Views are not supported".to_string(),
        ))
    }

    /// Register a materialized view with the catalog if it doesn't exist.
    async fn create_materialized_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: IcebergCreateMaterializedView,
    ) -> Result<IcebergMaterializedView, IcebergError> {
        Err(IcebergError::NotSupported(
            "Materialized views are not supported".to_string(),
        ))
    }

    /// perform commit table operation
    async fn update_table(
        self: Arc<Self>,
        commit: IcebergCommitTable,
    ) -> Result<IcebergTable, IcebergError> {
        let table_ident = IceBucketTableIdent::from_iceberg_ident(&commit.identifier);
        let table_update = IceBucketTableUpdate {
            requirements: commit.requirements,
            updates: commit.updates,
        };

        let rwobject = self
            .metastore
            .update_table(&table_ident, table_update)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;

        let iceberg_table = IcebergTable::new(
            commit.identifier.clone(),
            self.clone(),
            rwobject.metadata.clone(),
        )
        .await?;
        Ok(iceberg_table)
    }

    /// perform commit view operation
    async fn update_view(
        self: Arc<Self>,
        _commit: IcebergCommitView<Option<()>>,
    ) -> Result<IcebergView, IcebergError> {
        Err(IcebergError::NotSupported(
            "Views are not supported".to_string(),
        ))
    }

    /// perform commit view operation
    async fn update_materialized_view(
        self: Arc<Self>,
        _commit: IcebergCommitView<IcebergFullIdentifier>,
    ) -> Result<IcebergMaterializedView, IcebergError> {
        Err(IcebergError::NotSupported(
            "Materialized views are not supported".to_string(),
        ))
    }

    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _metadata_location: &str,
    ) -> Result<IcebergTable, IcebergError> {
        todo!()
    }

    /// Return the associated object store for a bucket
    fn object_store(&self, _bucket: IcebergBucket) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}
