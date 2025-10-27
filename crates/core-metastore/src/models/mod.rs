use std::ops::Deref;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod database;
pub mod schema;
pub mod table;
pub mod volumes;

pub use database::*;
pub use schema::*;
pub use table::*;

pub use volumes::*;

use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RwObject<T>
where
    T: Eq + PartialEq,
{
    #[serde(flatten)]
    pub data: T,
	pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// impl<T> Expression for RwObject<T>
// where
//     T: Expression,
// {
//     type SqlType = T::SqlType;
// }

impl<T> RwObject<T>
where
    T: Eq + PartialEq,
{
    pub fn new(data: T) -> RwObject<T> {
        let now = chrono::Utc::now();
		let id = Uuid::new_v4();
        Self {
            data,
			id,
            created_at: now,
            updated_at: now,
        }
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
