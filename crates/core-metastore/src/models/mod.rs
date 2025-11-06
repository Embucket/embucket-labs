use std::ops::Deref;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::{Result, NoNamedIdSnafu};
use snafu::OptionExt;

pub mod database;
pub mod schema;
pub mod table;
pub mod volumes;

pub use database::*;
pub use schema::*;
pub use table::*;

pub use volumes::*;

const MAP_ID: &str = "id";
const MAP_VOLUME_ID: &str = "volume_id";
const MAP_DATABASE_ID: &str = "database_id";
const MAP_SCHEMA_ID: &str = "schema_id";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RwObject<T>
where
    T: Eq + PartialEq,
{
    #[serde(flatten)]
    pub data: T,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub ids: HashMap<String, i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl RwObject<Database> {
    pub fn with_volume_id(self, id: i64) -> Self {
        self.with_named_id(MAP_VOLUME_ID.to_string(), id)
    }

    pub fn volume_id(&self) -> Result<i64> {
        self.named_id(MAP_VOLUME_ID)
    }
}

impl RwObject<Schema> {
    pub fn with_database_id(self, id: i64) -> Self {
        self.with_named_id(MAP_DATABASE_ID.to_string(), id)
    }
    
    pub fn database_id(&self) -> Result<i64> {
        self.named_id(MAP_DATABASE_ID)
    }

    pub fn schema_id(&self) -> Result<i64> {
        self.named_id(MAP_SCHEMA_ID)
    }
}

impl RwObject<Table> {
    pub fn with_database_id(self, id: i64) -> Self {
        self.with_named_id(MAP_DATABASE_ID.to_string(), id)
    }
    
    pub fn with_schema_id(self, id: i64) -> Self {
        self.with_named_id(MAP_SCHEMA_ID.to_string(), id)
    }

    pub fn database_id(&self) -> Result<i64> {
        self.named_id(MAP_DATABASE_ID)
    }

    pub fn schema_id(&self) -> Result<i64> {
        self.named_id(MAP_SCHEMA_ID)
    }
}

impl<T> RwObject<T>
where
    T: Eq + PartialEq + Serialize,
{
    pub fn new(data: T) -> RwObject<T> {
        let now = chrono::Utc::now();
        Self {
            data,
            ids: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_id(self, id: i64) -> Self {
        self.with_named_id(MAP_ID.to_string(), id)
    }

    pub fn id(&self) -> Result<i64> {
        self.named_id(MAP_ID)
    }

    fn with_named_id(self, name: String, id: i64) -> Self {
        let mut ids = self.ids;
        ids.insert(name, id);
        Self { ids, ..self }
    }

    fn named_id(&self, name: &str) -> Result<i64> {
        self.ids.get(name).cloned().context(NoNamedIdSnafu {
            name,
            object: serde_json::to_string(self).unwrap_or_default(),
        })
    }

    pub fn with_created_at(self, created_at: DateTime<Utc>) -> Self {
        Self { created_at, ..self }
    }

    pub fn with_updated_at(self, updated_at: DateTime<Utc>) -> Self {
        Self { updated_at, ..self }
    }

    pub fn update(&mut self, data: T) {
        if data != self.data {
            self.data = data;
            self.updated_at = chrono::Utc::now();
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now();
    }
}

impl<T> Deref for RwObject<T>
where
    T: Eq + PartialEq,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}