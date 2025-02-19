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

use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, Table};
use catalog::models;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateDatabasePayload {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

impl CreateDatabasePayload {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(name: String) -> Self {
        Self {
            name,
            properties: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    pub id: Uuid,
    pub name: String,
    pub tables: Vec<Table>,
    pub storage_profile: StorageProfile,
    pub properties: HashMap<String, String>,
    pub warehouse_id: Uuid,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: DateTime<chrono::Utc>,
    pub updated_at: DateTime<chrono::Utc>,
}

impl Database {
    pub fn with_details(
        &mut self,
        warehouse_id: Uuid,
        profile: &StorageProfile,
        mut tables: Vec<Table>,
    ) {
        self.storage_profile = profile.clone();

        let mut total_statistics = Statistics::default();
        for t in &mut tables {
            t.with_details(warehouse_id, profile.clone(), self.name.clone());
            total_statistics = total_statistics.aggregate(&t.statistics);
        }
        total_statistics.database_count = Some(1);

        self.statistics = total_statistics;
        self.warehouse_id = warehouse_id;
        self.tables = tables;

        if let Some(created_at) = self.properties.get("created_at") {
            if let Ok(created_at) = chrono::DateTime::parse_from_rfc3339(created_at) {
                self.created_at = DateTime::from(created_at);
            }
        }
        if let Some(updated_at) = self.properties.get("updated_at") {
            if let Ok(updated_at) = chrono::DateTime::parse_from_rfc3339(updated_at) {
                self.updated_at = DateTime::from(updated_at);
            }
        }
    }
}

impl From<models::Database> for Database {
    fn from(db: models::Database) -> Self {
        Self {
            id: get_database_id(&db.ident),
            name: db.ident.namespace.join("."),
            tables: vec![],
            warehouse_id: Uuid::default(),
            created_at: DateTime::default(),
            updated_at: DateTime::default(),
            statistics: Statistics::default(),
            compaction_summary: None,
            properties: db.properties,
            storage_profile: StorageProfile::default(),
        }
    }
}

#[must_use]
pub fn get_database_id(ident: &models::DatabaseIdent) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, ident.namespace.join("__").as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompactionSummary {
    pub compactions: i32,
    pub starting_files: i32,
    pub rewritten_files: i32,
    pub file_percent: i32,
    pub starting_size: i32,
    pub rewritten_size: i32,
    pub size_change: i32,
    pub size_percent: i32,
}

impl CompactionSummary {
    #[allow(clippy::new_without_default, clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        compactions: i32,
        starting_files: i32,
        rewritten_files: i32,
        file_percent: i32,
        starting_size: i32,
        rewritten_size: i32,
        size_change: i32,
        size_percent: i32,
    ) -> Self {
        Self {
            compactions,
            starting_files,
            rewritten_files,
            file_percent,
            starting_size,
            rewritten_size,
            size_change,
            size_percent,
        }
    }
}
