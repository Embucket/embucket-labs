use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, TableEntity, TableShort};
use crate::http::ui::models::warehouse::{Warehouse, WarehouseEntity, WarehouseExtended};
use catalog::models;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

pub const DATABASE_NAME: &str = "database_name";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct CreateDatabasePayload {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
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
    pub properties: Option<HashMap<String, String>>,
    pub id: Uuid,
    pub warehouse_id: Uuid,
}

impl Database {
    #[allow(clippy::new_without_default)]
    pub fn new(name: String, id: Uuid, warehouse_id: Uuid) -> Database {
        Database {
            name,
            properties: None,
            id,
            warehouse_id,
        }
    }
}

impl From<models::Database> for Database {
    fn from(db: models::Database) -> Self {
        Database {
            id: get_database_id(db.ident.clone()),
            name: db.ident.namespace.first().unwrap().to_string(),
            properties: db.properties.into(),
            warehouse_id: *db.ident.warehouse.id(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseDashboard {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    pub id: Uuid,
    pub warehouse_id: Uuid,
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
        id: Uuid,
        warehouse_id: Uuid,
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
    pub id: Uuid,
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
        id: Uuid,
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

impl From<models::Database> for DatabaseEntity {
    fn from(db: models::Database) -> Self {
        DatabaseEntity {
            id: get_database_id(db.ident.clone()),
            name: db.ident.namespace.first().unwrap().to_string(),
            created_at: Default::default(),
            updated_at: Default::default(),
            statistics: Default::default(),
            compaction_summary: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseExtended {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    pub id: Uuid,
    pub warehouse_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub warehouse: WarehouseExtended,
}

impl DatabaseExtended {
    pub fn new(
        profile: StorageProfile,
        warehouse: Warehouse,
        database: Database,
    ) -> DatabaseExtended {
        DatabaseExtended {
            id: database.id,
            name: database.name,
            warehouse_id: database.warehouse_id,
            properties: database.properties,
            statistics: None,
            compaction_summary: None,
            created_at: Default::default(),
            updated_at: Default::default(),
            warehouse: WarehouseExtended::new(warehouse, profile),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
pub struct DatabaseShort {
    pub id: Uuid,
    pub name: String,
    pub tables: Vec<TableShort>,
}

pub fn get_database_id(ident: models::DatabaseIdent) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        ident.namespace.first().unwrap().to_string().as_bytes(),
    )
}

impl DatabaseShort {
    #[allow(clippy::new_without_default)]
    pub fn new(id: Uuid, name: String, tables: Vec<TableShort>) -> DatabaseShort {
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
