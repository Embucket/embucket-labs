#![allow(unused_qualifications)]

use crate::http::ui::models::database::{CompactionSummary, DatabaseEntity, DatabaseShort};
use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::Statistics;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct Warehouse {
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
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl WarehouseDashboard {
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
        storage_profile: StorageProfile,
        databases: Vec<DatabaseEntity>,
        statistics: Statistics,
    ) -> WarehouseDashboard {
        WarehouseDashboard {
            name,
            storage_profile_id,
            key_prefix,
            id,
            external_id,
            location,
            created_at,
            updated_at,
            storage_profile,
            databases,
            statistics,
            compaction_summary: None,
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
    pub fn new(
        name: String,
        storage_profile_id: uuid::Uuid,
        key_prefix: String,
        id: uuid::Uuid,
        external_id: uuid::Uuid,
        location: String,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        storage_profile: StorageProfile,
    ) -> WarehouseEntity {
        WarehouseEntity {
            name,
            storage_profile_id,
            key_prefix,
            id,
            external_id,
            location,
            created_at,
            updated_at,
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
    pub fn new(
        name: String,
        storage_profile_id: uuid::Uuid,
        key_prefix: String,
        id: uuid::Uuid,
        external_id: uuid::Uuid,
        location: String,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        storage_profile: StorageProfile,
    ) -> WarehouseExtended {
        WarehouseExtended {
            name,
            storage_profile_id,
            key_prefix,
            id,
            external_id,
            location,
            created_at,
            updated_at,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
