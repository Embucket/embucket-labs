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

use crate::execution::catalogs::metastore::CatalogProviderCache;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion_common::{exec_err, DataFusionError, Result as DFResult};
use icebucket_metastore::Metastore;
use std::any::Any;
use std::sync::Arc;

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
