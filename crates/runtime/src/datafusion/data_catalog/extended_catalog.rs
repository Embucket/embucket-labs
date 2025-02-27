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

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::{datasource::TableProvider, error::DataFusionError};
use datafusion_expr::TableType;
use datafusion_iceberg::catalog::mirror::Mirror;
use iceberg_rust::catalog::{identifier::Identifier, namespace::Namespace, Catalog};
use std::sync::Arc;

#[derive(Debug)]
pub struct ExtendedIcebergCatalog {
    pub base: Arc<Mirror>,
    pub memory_catalog_provider: MemoryCatalogProvider,
}

impl ExtendedIcebergCatalog {
    /// Creates a new `ExtendedIcebergCatalog` with support for both Iceberg and memory tables
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

    pub fn new_sync(catalog: &Arc<dyn Catalog>, branch: Option<String>) -> Self {
        let base_mirror = Mirror::new_sync(catalog.clone(), branch);

        Self {
            base: Arc::new(base_mirror),
            memory_catalog_provider: MemoryCatalogProvider::new(),
        }
    }

    pub fn table_names(&self, namespace: &Namespace) -> Vec<Identifier> {
        let mut table_names = self.base.table_names(namespace).unwrap_or_else(|_| vec![]);
        let memory_tables: Vec<Identifier> = self
            .memory_catalog_provider
            .schema(namespace.to_string().as_str())
            .map_or_else(
                Vec::new,
                |schema| {
                    schema
                        .table_names()
                        .into_iter()
                        .filter_map(|name| Identifier::try_new(&[name], None).ok())
                        .collect()
                },
            );

        table_names.extend(memory_tables);
        table_names
    }

    pub fn schema_names(&self, parent: Option<&str>) -> Result<Vec<Namespace>, DataFusionError> {
        self.base.schema_names(parent)
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

    #[allow(clippy::expect_used)]
    pub fn table_exists(&self, identifier: &Identifier) -> bool {
        self.base.table_exists(identifier.clone())
            || self
                .memory_catalog_provider
                .schema(identifier.namespace().to_string().as_str())
                .expect("Schema does not exist")
                .table_exist(identifier.name())
    }

    #[allow(clippy::expect_used)]
    pub fn register_table(
        &self,
        identifier: &Identifier,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match table.table_type() {
            // TODO: We should register schema in base catalog in case it's temporary table
            TableType::Temporary => self
                .memory_catalog_provider
                .schema(identifier.namespace().to_string().as_str())
                .unwrap_or_else(|| {
                    let _ = self.memory_catalog_provider.register_schema(
                        identifier.namespace().to_string().as_str(),
                        Arc::new(MemorySchemaProvider::new()),
                    );
                    self.memory_catalog_provider
                        .schema(identifier.namespace().to_string().as_str())
                        .expect("Schema does not exist")
                })
                .register_table(identifier.name().to_string(), table),
            TableType::Base => self.base.register_table(identifier.clone(), table.clone()),
            TableType::View => Err(DataFusionError::Execution(
                "Unsupported table type for registration".to_string(),
            )),
        }
    }

    #[allow(clippy::expect_used)]
    pub fn deregister_table(
        &self,
        identifier: &Identifier,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.base.deregister_table(identifier.clone()).map_or_else(
            |_| {
                self.memory_catalog_provider
                    .schema(identifier.namespace().to_string().as_str())
                    .expect("Schema does not exist")
                    .deregister_table(identifier.name())
            },
            Ok,
        )
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.base.catalog()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::data_catalog::catalog::tests::{
        prepare_mock_rest_catalog, TEST_WAREHOUSE_ID,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_extended_iceberg_catalog_new() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let extended_catalog = ExtendedIcebergCatalog::new(catalog, None).await;
        assert!(extended_catalog.is_ok());
    }

    #[tokio::test]
    async fn test_extended_iceberg_catalog_new_sync() {
        let catalog = prepare_mock_rest_catalog(TEST_WAREHOUSE_ID).await;
        let extended_catalog = ExtendedIcebergCatalog::new_sync(catalog, None);
        assert!(Arc::strong_count(&extended_catalog.base) > 0);
    }
}
