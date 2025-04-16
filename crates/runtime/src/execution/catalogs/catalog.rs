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

use std::{any::Any, sync::Arc};

use crate::execution::catalogs::metastore::CatalogProviderCache;
use crate::execution::catalogs::schema::DFSchema;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use embucket_metastore::Metastore;
use iceberg_rust::catalog::Catalog as IcebergCatalog;

pub struct DFCatalog {
    pub ident: String,
    pub metastore: Arc<dyn Metastore>,
    pub mirror: Arc<CatalogProviderCache>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
}

impl DFCatalog {
    #[must_use]
    pub fn catalog(&self) -> Arc<dyn IcebergCatalog> {
        self.iceberg_catalog.clone()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for DFCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFCatalog")
            .field("ident", &self.ident)
            .finish()
    }
}

impl CatalogProvider for DFCatalog {
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
                let schema: Arc<dyn SchemaProvider> = Arc::new(DFSchema {
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
