use crate::http::ui::models::table::{Statistics, TableEntity, TableShort};
use crate::http::ui::models::warehouse::{WarehouseEntity, WarehouseExtended};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct CreateDatabasePayload {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

impl CreateDatabasePayload {
    #[allow(clippy::new_without_default)]
    pub fn new(name: String) -> CreateDatabasePayload {
        CreateDatabasePayload {
            name,
            properties: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct Database {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
    pub id: uuid::Uuid,
    pub warehouse_id: uuid::Uuid,
}

impl Database {
    #[allow(clippy::new_without_default)]
    pub fn new(name: String, id: uuid::Uuid, warehouse_id: uuid::Uuid) -> Database {
        Database {
            name,
            properties: None,
            id,
            warehouse_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseDashboard {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
    pub id: uuid::Uuid,
    pub warehouse_id: uuid::Uuid,
    pub warehouse: WarehouseEntity,
    pub tables: Vec<TableEntity>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl DatabaseDashboard {
    #[allow(clippy::new_without_default)]
    pub fn new(
        name: String,
        id: uuid::Uuid,
        warehouse_id: uuid::Uuid,
        warehouse: WarehouseEntity,
        tables: Vec<TableEntity>,
        statistics: Statistics,
    ) -> DatabaseDashboard {
        DatabaseDashboard {
            name,
            properties: None,
            id,
            warehouse_id,
            warehouse,
            tables,
            statistics,
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseEntity {
    pub id: uuid::Uuid,
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub statistics: Statistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
}

impl DatabaseEntity {
    #[allow(clippy::new_without_default)]
    pub fn new(
        id: uuid::Uuid,
        name: String,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        statistics: Statistics,
    ) -> DatabaseEntity {
        DatabaseEntity {
            id,
            name,
            created_at,
            updated_at,
            statistics,
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseExtended {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
    pub id: uuid::Uuid,
    pub warehouse_id: uuid::Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub warehouse: WarehouseExtended,
}

impl DatabaseExtended {
    #[allow(clippy::new_without_default)]
    pub fn new(
        name: String,
        id: uuid::Uuid,
        warehouse_id: uuid::Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
        updated_at: chrono::DateTime<chrono::Utc>,
        warehouse: WarehouseExtended,
    ) -> DatabaseExtended {
        DatabaseExtended {
            name,
            properties: None,
            id,
            warehouse_id,
            statistics: None,
            compaction_summary: None,
            created_at,
            updated_at,
            warehouse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseShort {
    pub id: uuid::Uuid,
    pub name: String,
    pub tables: Vec<TableShort>,
}

impl DatabaseShort {
    #[allow(clippy::new_without_default)]
    pub fn new(id: uuid::Uuid, name: String, tables: Vec<TableShort>) -> DatabaseShort {
        DatabaseShort { id, name, tables }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
    #[allow(clippy::new_without_default)]
    pub fn new(
        compactions: i32,
        starting_files: i32,
        rewritten_files: i32,
        file_percent: i32,
        starting_size: i32,
        rewritten_size: i32,
        size_change: i32,
        size_percent: i32,
    ) -> CompactionSummary {
        CompactionSummary {
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
