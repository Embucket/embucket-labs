use std::ops::Deref;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

pub mod database;
pub mod schema;
pub mod table;
pub mod volumes;

pub use database::*;
pub use schema::*;
pub use table::*;

pub use volumes::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RwObject<T>
where
    T: Eq + PartialEq,
{
    #[serde(flatten)]
    pub data: T,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl<T> RwObject<T>
where
    T: Eq + PartialEq,
{
    pub fn new(data: T) -> Self {
        let now = chrono::Utc::now().naive_utc();
        Self {
            data,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn update(&mut self, data: T) {
        if data != self.data {
            self.data = data;
            self.updated_at = chrono::Utc::now().naive_utc();
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now().naive_utc();
    }
}

impl<T> Deref for RwObject<T>
where
    T: Eq + PartialEq,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/*#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RwObjectVec<T>(pub Vec<RwObject<T>>) where T: Eq + PartialEq;

impl<T> Deref for RwObjectVec<T> where T: Eq + PartialEq
{
    type Target = Vec<RwObject<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + PartialEq> From<Vec<RwObject<T>>> for RwObjectVec<T> {
    fn from(rw_objects: Vec<RwObject<T>>) -> Self {
        Self(rw_objects)
    }
}

impl<T: Eq + PartialEq> From<RwObjectVec<T>> for Vec<RwObject<T>> {
    fn from(rw_objects: RwObjectVec<T>) -> Self {
        rw_objects.0
    }
}

impl<T: Eq + PartialEq> IntoIterator for RwObjectVec<T> {
    type Item = RwObject<T>;
    type IntoIter = std::vec::IntoIter<RwObject<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}*/
