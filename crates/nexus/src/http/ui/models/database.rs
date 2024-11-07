use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, Table};
use catalog::models;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
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
    pub fn with_details(&mut self, warehouse_id: Uuid, profile: StorageProfile, mut tables: Vec<Table>) {
        self.storage_profile = profile.clone();

        let mut total_statistics = Statistics::default();
        tables.iter_mut().for_each(|t| {
            t.with_details(warehouse_id.clone(), profile.clone(), self.name.clone());
            total_statistics = total_statistics.aggregate(&t.statistics);
        });
        total_statistics.database_count = Some(1);

        self.statistics = total_statistics;
        self.warehouse_id = warehouse_id;
        self.tables = tables;

        self.properties.get("created_at").map(|created_at| {
            self.created_at = DateTime::from(chrono::DateTime::parse_from_rfc3339(created_at).unwrap());
        });
        self.properties.get("updated_at").map(|updated_at| {
            self.updated_at = DateTime::from(chrono::DateTime::parse_from_rfc3339(updated_at).unwrap());
        });
    }
}

impl From<models::Database> for Database {
    fn from(db: models::Database) -> Self {
        Database {
            id: get_database_id(db.ident.clone()),
            name: db.ident.namespace.first().unwrap().to_string(),
            tables: vec![],
            warehouse_id: Default::default(),
            created_at: Default::default(),
            updated_at: Default::default(),
            statistics: Default::default(),
            compaction_summary: None,
            properties: db.properties,
            storage_profile: Default::default(),
        }
    }
}

pub fn get_database_id(ident: models::DatabaseIdent) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_DNS,
        ident.namespace.first().unwrap().to_string().as_bytes(),
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
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
