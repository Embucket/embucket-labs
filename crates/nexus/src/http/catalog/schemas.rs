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

use crate::http::utils::update_properties_timestamps;
pub use catalog::models::Config;
use catalog::models::Table;
use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
pub use iceberg::NamespaceIdent;
pub use iceberg::{TableIdent, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Namespace {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    /// Configured string to string map of properties for the namespace
    pub properties: Option<std::collections::HashMap<String, String>>,
}

impl From<catalog::models::Database> for Namespace {
    fn from(db: catalog::models::Database) -> Self {
        Self {
            namespace: db.ident.namespace,
            properties: Some(db.properties),
        }
    }
}

/// Result used when a table is successfully loaded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableResult {
    /// May be null if the table is staged as part of a transaction
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    pub config: Option<HashMap<String, String>>,
}

impl From<Table> for TableResult {
    fn from(table: Table) -> Self {
        Self {
            metadata_location: Some(table.metadata_location),
            metadata: table.metadata,
            config: Some(HashMap::default()), // FIXME: populate this
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCreateRequest {
    pub name: String,
    pub location: Option<String>,
    pub schema: Schema,
    pub partition_spec: Option<UnboundPartitionSpec>,
    pub write_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}

impl From<TableCreateRequest> for catalog::models::TableCreation {
    fn from(schema: TableCreateRequest) -> Self {
        let mut properties = schema.properties.unwrap_or_default();
        update_properties_timestamps(&mut properties);

        Self {
            name: schema.name,
            location: schema.location,
            schema: schema.schema,
            partition_spec: schema.partition_spec,
            sort_order: schema.write_order,
            properties,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRegisterRequest {
    pub name: String,
    pub metadata_location: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableListResponse {
    pub identifiers: Vec<TableIdent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceListResponse {
    pub namespaces: Vec<NamespaceIdent>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TableCommitRequest {
    pub identifier: Option<TableIdent>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
    pub properties: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCommitResponse {
    pub metadata_location: String,
    pub metadata: TableMetadata,
    pub config: Option<HashMap<String, String>>,
}

#[derive(serde::Deserialize, Debug)]
pub struct GetConfigQueryParams {
    pub warehouse: Option<Uuid>,
}
