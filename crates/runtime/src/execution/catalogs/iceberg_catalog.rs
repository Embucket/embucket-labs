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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
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
use icebucket_metastore::error::{MetastoreError, MetastoreResult};
use icebucket_metastore::{
    IceBucketSchema, IceBucketSchemaIdent, IceBucketTableCreateRequest, IceBucketTableIdent,
    IceBucketTableUpdate, Metastore,
};
use object_store::ObjectStore;
use snafu::ResultExt;

#[derive(Debug)]
pub struct IceBucketIcebergBridge {
    pub metastore: Arc<dyn Metastore>,
    pub database: String,
    pub object_store: Arc<dyn ObjectStore>,
}

impl IceBucketIcebergBridge {
    pub(crate) fn new(metastore: Arc<dyn Metastore>, database: String) -> MetastoreResult<Self> {
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
            .context(crate::execution::error::MetastoreSnafu)
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
