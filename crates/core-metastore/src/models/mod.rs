use std::ops::Deref;

use crate::error::{NoNamedIdSnafu, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use std::collections::HashMap;

pub mod database;
pub mod schema;
pub mod table;
pub mod volumes;

pub use database::*;
pub use schema::*;
pub use table::*;
pub use volumes::*;

const MAP_VOLUME_ID: &str = "volume_id";
const MAP_DATABASE_ID: &str = "database_id";
const MAP_SCHEMA_ID: &str = "schema_id";
const MAP_TABLE_ID: &str = "table_id";

pub trait NamedId {
    fn type_name() -> &'static str;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RwObject<T>
where
    T: Eq + PartialEq
{
    #[serde(flatten)]
    pub data: T,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub ids: HashMap<String, i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl<T> RwObject<T>
where
    T: Eq + PartialEq + Serialize
{
    #[allow(clippy::use_self)]
    pub fn new(data: T) -> RwObject<T> {
        let now = chrono::Utc::now();
        Self {
            data,
            ids: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    fn with_named_id(self, name: &str, id: i64) -> Self {
        let mut ids = self.ids;
        ids.insert(name.to_string(), id);
        Self { ids, ..self }
    }

    fn named_id(&self, name: &str) -> Result<i64> {
        self.ids.get(name).copied().context(NoNamedIdSnafu {
            name,
            object: serde_json::to_string(self).unwrap_or_default(),
        })
    }

    #[must_use]
    pub fn with_created_at(self, created_at: DateTime<Utc>) -> Self {
        Self { created_at, ..self }
    }

    #[must_use]
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