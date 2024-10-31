use crate::http::ui::models::storage_profile::StorageProfile;
use crate::http::ui::models::table::{Statistics, Table};
use catalog::models;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
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
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Database {
    pub id: Uuid,
    pub name: String,
    pub tables: Vec<Table>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_profile: Option<StorageProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_summary: Option<CompactionSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Database {
    pub fn with_details(&mut self, profile: Option<StorageProfile>, tables: Option<Vec<Table>>) {
        if profile.is_some() {
            self.storage_profile = profile;
        }

        if tables.is_some() {
            let mut total_statistics = Statistics::default();

            for t in tables.clone().unwrap() {
                total_statistics = total_statistics.aggregate(&t.statistics.unwrap_or_default());
            }
            total_statistics.database_count = Some(1);
            self.statistics = Option::from(total_statistics);
            self.tables = tables.unwrap();
        }
    }
}

impl From<models::Database> for Database {
    fn from(db: models::Database) -> Self {
        Database {
            id: get_database_id(db.ident.clone()),
            name: db.ident.namespace.first().unwrap().to_string(),
            tables: vec![],
            warehouse_id: None,
            created_at: Default::default(),
            updated_at: Default::default(),
            statistics: Default::default(),
            compaction_summary: None,
            properties: None,
            storage_profile: None,
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
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
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
