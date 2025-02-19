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

#![allow(unused_qualifications)]

use crate::http::ui::models::database::{CompactionSummary, Database};
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::Statistics;
use chrono::{DateTime, Utc};
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Navigation {
    pub warehouses: Vec<Warehouse>,
}

impl Navigation {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(warehouses: Vec<Warehouse>) -> Self {
        Self { warehouses }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateWarehousePayload {
    #[validate(length(min = 1))]
    pub name: String,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub key_prefix: String,
}

impl CreateWarehousePayload {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(name: String, storage_profile_id: uuid::Uuid, key_prefix: String) -> Self {
        Self {
            name,
            storage_profile_id,
            key_prefix,
        }
    }
}

impl From<CreateWarehousePayload> for models::WarehouseCreateRequest {
    fn from(payload: CreateWarehousePayload) -> Self {
        Self {
            prefix: payload.key_prefix,
            name: payload.name,
            storage_profile_id: payload.storage_profile_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Warehouse {
    pub id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub name: String,
    pub databases: Vec<Database>,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub key_prefix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub storage_profile: StorageProfile,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl From<control_plane::models::Warehouse> for Warehouse {
    fn from(warehouse: control_plane::models::Warehouse) -> Self {
        Self {
            id: warehouse.id,
            key_prefix: warehouse.prefix,
            name: warehouse.name,
            location: Option::from(warehouse.location),
            storage_profile_id: warehouse.storage_profile_id,
            created_at: DateTime::from_naive_utc_and_offset(warehouse.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(warehouse.updated_at, Utc),
            storage_profile: StorageProfile::default(),
            statistics: Statistics::default(),
            external_id: Option::default(),
            compaction_summary: None,
            databases: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WarehousesDashboard {
    pub warehouses: Vec<Warehouse>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl WarehousesDashboard {
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub const fn new(warehouses: Vec<Warehouse>, statistics: Statistics) -> Self {
        Self {
            warehouses,
            statistics,
            compaction_summary: None,
        }
    }
}
