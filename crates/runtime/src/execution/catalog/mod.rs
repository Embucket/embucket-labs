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

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_common::{exec_err, DataFusionError, Result as DFResult};
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
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
use icebucket_metastore::{
    error::MetastoreResult, IceBucketSchema, IceBucketSchemaIdent, IceBucketTableIdent, Metastore,
};
use object_store::ObjectStore;

#[derive(Clone)]
pub struct IceBucketDFMetastore {
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<DashMap<String, DashMap<String, DashSet<String>>>>,
}

impl IceBucketDFMetastore {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            metastore,
            mirror: Arc::new(DashMap::new()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn refresh(&self) -> MetastoreResult<()> {
        let mut seen: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::default();

        let databases = self.metastore.list_databases().await?;
        for database in databases {
            let schemas = self.metastore.list_schemas(&database.ident).await?;
            for schema in schemas {
                let tables = self.metastore.list_tables(&schema.ident).await?;
                for table in tables {
                    let db_entry = self
                        .mirror
                        .entry(database.ident.clone())
                        .or_insert(DashMap::new());
                    let mirror_entry = db_entry
                        .entry(schema.ident.schema.clone())
                        .insert(DashSet::default());
                    mirror_entry.insert(table.ident.table.clone());
                    let seen_entry = seen
                        .entry(database.ident.clone())
                        .or_default()
                        .entry(schema.ident.schema.clone())
                        .or_default();
                    seen_entry.insert(table.ident.table.clone());
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
                                tables_to_remove.push(table.clone());
                            }
                        }

                        for table in tables_to_remove {
                            schema.remove(&table);
                        }
                        if schema.value().is_empty() {
                            db.remove(schema.key());
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

// Explore using AsyncCatalogProviderList alongside CatalogProviderList
impl CatalogProviderList for IceBucketDFMetastore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // This is currently a NOOP because we don't support registering new catalogs yet
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.mirror.iter().map(|e| e.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if !self.mirror.contains_key(name) {
            return None;
        }
        let catalog: Arc<dyn CatalogProvider> = Arc::new(IceBucketDFCatalog {
            ident: name.to_string(),
            metastore: self.metastore.clone(),
            mirror: self.mirror.clone(),
        });
        Some(catalog)
    }
}

pub struct IceBucketDFCatalog {
    pub ident: String,
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<DashMap<String, DashMap<String, DashSet<String>>>>,
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
        self.mirror
            .get(&self.ident)
            .map(|db| db.iter().map(|schema| schema.key().clone()).collect())
            .unwrap_or_default()
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
    pub mirror: Arc<DashMap<String, DashMap<String, DashSet<String>>>>,
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
impl datafusion::catalog::SchemaProvider for IceBucketDFSchema {
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
        let table_ident = IceBucketTableIdent {
            schema: self.schema.clone(),
            database: self.database.clone(),
            table: name.to_string(),
        };
        let ident_clone = table_ident.clone();
        let table_object_store = self
            .metastore
            .table_object_store(&table_ident)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if let Some(object_store) = table_object_store {
            let bridge = Arc::new(IceBucketIcebergBridge {
                metastore: self.metastore.clone(),
                ident: ident_clone,
                object_store: object_store.clone(),
            });

            let ib_identifier =
                IcebergIdentifier::new(&[self.database.clone(), self.schema.clone()], name);
            let tabular = bridge
                .load_tabular(&ib_identifier)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let dftable: Arc<dyn TableProvider> =
                Arc::new(IcebergDataFusionTable::new(tabular, None, None, None));
            Ok(Some(dftable))
        } else {
            Ok(None)
        }
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
        let table_ident = IceBucketTableIdent {
            schema: self.schema.clone(),
            database: self.database.clone(),
            table: name.to_string(),
        };
        tokio::runtime::Handle::current().block_on(async {
            self.metastore
                .get_table(&table_ident)
                .await
                .unwrap_or_default()
                .is_some()
        })
    }
}

#[derive(Debug)]
pub struct IceBucketIcebergBridge {
    pub metastore: Arc<dyn Metastore>,
    pub ident: IceBucketTableIdent,
    pub object_store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl IcebergCatalog for IceBucketIcebergBridge {
    /// Name of the catalog
    fn name(&self) -> &str {
        &self.ident.database
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
            database: self.ident.database.clone(),
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
            database: self.ident.database.clone(),
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
            database: self.ident.database.clone(),
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
            database: self.ident.database.clone(),
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
            database: self.ident.database.clone(),
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
            database: self.ident.database.clone(),
            schema: namespace.join(""),
        };
        Ok(self
            .metastore
            .list_tables(&schema_ident)
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
            .list_databases()
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        for database in databases {
            let schemas = self
                .metastore
                .list_schemas(&database.ident)
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
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
        Ok(self
            .metastore
            .get_table(&table_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .is_some())
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
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
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
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
        _identifier: IcebergIdentifier,
        _create_table: IcebergCreateTable,
    ) -> Result<IcebergTable, IcebergError> {
        todo!()
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
        _commit: IcebergCommitTable,
    ) -> Result<IcebergTable, IcebergError> {
        todo!()
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
