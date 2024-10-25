#![allow(unused_qualifications)]

use crate::http::ui::models::database::{CompactionSummary, DatabaseEntity, DatabaseShort};
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::Statistics;
use chrono::{DateTime, Utc};
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct Navigation {
    pub warehouses: Vec<WarehouseShort>,
}

impl Navigation {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouses: Vec<WarehouseShort>) -> Navigation {
        Navigation { warehouses }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
pub struct Warehouse {
    #[validate(length(min = 1))]
    pub name: String,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    #[serde(rename = "prefix")]
    pub key_prefix: String,
    pub id: uuid::Uuid,
    pub external_id: uuid::Uuid,
    pub location: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Warehouse {
    #[allow(clippy::new_without_default)]
    pub fn new(
        name: String,
        storage_profile_id: uuid::Uuid,
        key_prefix: String,
        id: uuid::Uuid,
        external_id: uuid::Uuid,
        location: String,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Warehouse {
        Warehouse {
            name,
            storage_profile_id,
            key_prefix,
            id,
            external_id,
            location,
            created_at,
            updated_at,
        }
    }
}

impl From<control_plane::models::Warehouse> for Warehouse {
    fn from(warehouse: control_plane::models::Warehouse) -> Self {
        Warehouse {
            id: warehouse.id,
            key_prefix: warehouse.prefix,
            name: warehouse.name,
            location: warehouse.location,
            storage_profile_id: warehouse.storage_profile_id,
            created_at: DateTime::from_naive_utc_and_offset(warehouse.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(warehouse.updated_at, Utc),
            external_id: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct WarehouseDashboard {
    #[validate(length(min = 1))]
    pub name: String,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub key_prefix: String,
    pub id: uuid::Uuid,
    pub external_id: uuid::Uuid,
    pub location: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub storage_profile: StorageProfile,
    pub databases: Vec<DatabaseEntity>,
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl WarehouseDashboard {
    #[allow(clippy::new_without_default)]
    pub fn new(
        warehouse: Warehouse,
        storage_profile: StorageProfile,
        databases: Vec<DatabaseEntity>,
    ) -> WarehouseDashboard {
        WarehouseDashboard {
            id: warehouse.id,
            key_prefix: warehouse.key_prefix,
            name: warehouse.name,
            location: warehouse.location,
            storage_profile_id: warehouse.storage_profile_id,
            created_at: warehouse.created_at,
            updated_at: warehouse.updated_at,
            external_id: warehouse.external_id,
            statistics: None,
            compaction_summary: None,
            storage_profile,
            databases: databases,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct WarehouseEntity {
    #[validate(length(min = 1))]
    pub name: String,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub key_prefix: String,
    pub id: uuid::Uuid,
    pub external_id: uuid::Uuid,
    pub location: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub storage_profile: StorageProfile,
}

impl WarehouseEntity {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouse: Warehouse, storage_profile: StorageProfile) -> WarehouseEntity {
        WarehouseEntity {
            id: warehouse.id,
            key_prefix: warehouse.key_prefix,
            name: warehouse.name,
            location: warehouse.location,
            storage_profile_id: warehouse.storage_profile_id,
            created_at: warehouse.created_at,
            updated_at: warehouse.updated_at,
            external_id: warehouse.external_id,
            storage_profile,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct WarehouseExtended {
    #[validate(length(min = 1))]
    pub name: String,
    pub storage_profile_id: uuid::Uuid,
    #[validate(length(min = 1))]
    pub key_prefix: String,
    pub id: uuid::Uuid,
    pub external_id: uuid::Uuid,
    pub location: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub storage_profile: StorageProfile,
}

impl WarehouseExtended {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouse: Warehouse, storage_profile: StorageProfile) -> WarehouseExtended {
        WarehouseExtended {
            id: warehouse.id,
            key_prefix: warehouse.key_prefix,
            name: warehouse.name,
            location: warehouse.location,
            storage_profile_id: warehouse.storage_profile_id,
            created_at: warehouse.created_at,
            updated_at: warehouse.updated_at,
            external_id: warehouse.external_id,
            statistics: None,
            compaction_summary: None,
            storage_profile,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct WarehouseShort {
    pub id: uuid::Uuid,
    pub name: String,
    pub databases: Vec<DatabaseShort>,
}

impl WarehouseShort {
    #[allow(clippy::new_without_default)]
    pub fn new(id: uuid::Uuid, name: String, databases: Vec<DatabaseShort>) -> WarehouseShort {
        WarehouseShort {
            id,
            name,
            databases,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Default, ToSchema)]
pub struct WarehousesDashboard {
    pub warehouses: Vec<WarehouseExtended>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl WarehousesDashboard {
    #[allow(clippy::new_without_default)]
    pub fn new(warehouses: Vec<WarehouseExtended>, statistics: Statistics) -> WarehousesDashboard {
        WarehousesDashboard {
            warehouses,
            statistics,
            compaction_summary: None,
        }
    }
}
