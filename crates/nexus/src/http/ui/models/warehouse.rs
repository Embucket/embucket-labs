#![allow(unused_qualifications)]

use crate::http::ui::models::database::{CompactionSummary, Database};
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::Statistics;
use chrono::{DateTime, Utc};
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Navigation {
    pub warehouses: Vec<Warehouse>,
}

impl Navigation {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouses: Vec<Warehouse>) -> Navigation {
        Navigation { warehouses }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
    pub fn new(
        name: String,
        storage_profile_id: uuid::Uuid,
        key_prefix: String,
    ) -> CreateWarehousePayload {
        CreateWarehousePayload {
            name,
            storage_profile_id,
            key_prefix,
        }
    }
}

impl From<CreateWarehousePayload> for models::WarehouseCreateRequest {
    fn from(payload: CreateWarehousePayload) -> Self {
        models::WarehouseCreateRequest {
            prefix: payload.key_prefix,
            name: payload.name,
            storage_profile_id: payload.storage_profile_id,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Warehouse {
    pub id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub name: String,
    pub databases: Option<Vec<Database>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_profile_id: Option<uuid::Uuid>,
    #[validate(length(min = 1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_profile: Option<StorageProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl Warehouse {
    pub fn with_details(&mut self, profile: Option<StorageProfile>, databases: Option<Vec<Database>>) {
        if profile.is_some() {
            self.storage_profile = profile;
        }
        if databases.is_some() {
            self.databases = databases;
        }
    }
}

impl From<control_plane::models::Warehouse> for Warehouse {
    fn from(warehouse: control_plane::models::Warehouse) -> Self {
        Warehouse {
            id: warehouse.id,
            key_prefix: Option::from(warehouse.prefix),
            name: warehouse.name,
            location: Option::from(warehouse.location),
            storage_profile_id: Option::from(warehouse.storage_profile_id),
            created_at: Option::from(DateTime::from_naive_utc_and_offset(warehouse.created_at, Utc)),
            updated_at: Option::from(DateTime::from_naive_utc_and_offset(warehouse.updated_at, Utc)),
            storage_profile: None,
            statistics: None,
            external_id: Default::default(),
            databases: None,
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WarehousesDashboard {
    pub warehouses: Vec<Warehouse>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl WarehousesDashboard {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouses: Vec<Warehouse>, statistics: Statistics) -> WarehousesDashboard {
        WarehousesDashboard {
            warehouses,
            statistics,
            compaction_summary: None,
        }
    }
}