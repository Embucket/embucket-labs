// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::de;
use serde_json::ser;
use slatedb::db::Db as SlateDb;
use slatedb::error::SlateDBError;
use snafu::prelude::*;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Snafu, Debug)]
//#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("SlateDB error: {source}"))]
    Database { source: SlateDBError },

    #[snafu(display("SlateDB error while fetching key {key}: {source}"))]
    KeyGet { key: String, source: SlateDBError },

    #[snafu(display("SlateDB error while deleting key {key}: {source}"))]
    KeyDelete { key: String, source: SlateDBError },

    #[snafu(display("SlateDB error while putting key {key}: {source}"))]
    KeyPut { key: String, source: SlateDBError },

    #[snafu(display("Error serializing value: {source}"))]
    SerializeValue { source: serde_json::Error },

    #[snafu(display("Deserialize error: {source}"))]
    DeserializeValue { source: serde_json::Error },

    #[snafu(display("Key Not found"))]
    KeyNotFound,

    #[snafu(display("Scan Failed: {source}"))]
    ScanFailed { source: SlateDBError },
}

type Result<T> = std::result::Result<T, Error>;

pub struct Db(Arc<SlateDb>);

impl Db {
    pub const fn new(db: Arc<SlateDb>) -> Self {
        Self(db)
    }

    #[allow(clippy::expect_used)]
    pub async fn memory() -> Self {
        let object_store = object_store::memory::InMemory::new();
        let db = SlateDb::open(
            object_store::path::Path::from("/"),
            std::sync::Arc::new(object_store),
        )
        .await
        .expect("Failed to open database");
        Self(Arc::new(db))
    }

    /// Closes the database connection.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn close(&self) -> Result<()> {
        self.0.close().await.context(DatabaseSnafu)?;
        Ok(())
    }

    /// Deletes a key-value pair from the database.
    ///
    /// # Errors
    ///
    /// This function will return a `DbError` if the underlying database operation fails.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.0.delete(key.as_bytes()).await.context(KeyDeleteSnafu {
            key: key.to_string(),
        })
    }

    /// Stores a key-value pair in the database.
    ///
    /// # Errors
    ///
    /// Returns a `SerializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn put<T: serde::Serialize + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let serialized = ser::to_vec(value).context(SerializeValueSnafu)?;
        self.0
            .put(key.as_bytes(), serialized.as_ref())
            .await
            .context(KeyPutSnafu {
                key: key.to_string(),
            })
    }

    /// Retrieves a value from the database by its key.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    /// Returns a `DeserializeError` if the value cannot be deserialized from JSON.
    pub async fn get<T: for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let value: Option<bytes::Bytes> =
            self.0.get(key.as_bytes()).await.context(KeyGetSnafu {
                key: key.to_string(),
            })?;
        value.map_or_else(
            || Ok(None),
            |bytes| de::from_slice(&bytes).context(DeserializeValueSnafu), //.map_err(|e| Error::Deserialize { source: e}),
        )
    }

    /// Retrieves a list of keys from the database.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    /// Returns a `DeserializeError` if the value cannot be deserialized from JSON.
    #[allow(clippy::unwrap_used)]
    pub async fn list_objects<T: for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Result<Vec<T>> {
        let start = format!("{key}/");
        let end = format!("{key}/\x7F");
        let range = Bytes::from(start)..Bytes::from(end);
        let mut iter = self.0.scan(range).await.context(ScanFailedSnafu)?;
        let mut objects: Vec<T> = vec![];
        while let Ok(Some(value)) = iter.next().await {
            let value = de::from_slice(&value.value).context(DeserializeValueSnafu)?;
            objects.push(value);
        }
        Ok(objects)
    }
}

impl From<Error> for iceberg::Error {
    fn from(e: Error) -> Self {
        Self::new(iceberg::ErrorKind::Unexpected, e.to_string()).with_source(e)
    }
}

#[async_trait]
pub trait Entity {
    fn id(&self) -> Uuid;
}

#[async_trait]
pub trait Repository {
    type Entity: Entity + Serialize + DeserializeOwned + Send + Sync;

    fn db(&self) -> &Db;

    async fn _create(&self, entity: &Self::Entity) -> Result<()> {
        let key = format!("{}/{}", Self::prefix(), entity.id());
        self.db().put(&key, &entity).await?;
        //self.db().list_append(Self::collection_key(), key).await?;
        Ok(())
    }

    async fn _get(&self, id: Uuid) -> Result<Self::Entity> {
        let key = format!("{}/{}", Self::prefix(), id);
        let entity = self.db().get(&key).await?;
        let entity = entity.ok_or(Error::KeyNotFound)?;
        Ok(entity)
    }

    async fn _delete(&self, id: Uuid) -> Result<()> {
        let key = format!("{}/{}", Self::prefix(), id);
        self.db().delete(&key).await?;
        //self.db().list_remove(Self::collection_key(), &key).await?;
        Ok(())
    }

    async fn _list(&self) -> Result<Vec<Self::Entity>> {
        let entities = self.db().list_objects(Self::collection_key()).await?;
        Ok(entities)
    }

    fn prefix() -> &'static str;
    fn collection_key() -> &'static str;
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod test {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestEntity {
        id: i32,
        name: String,
    }

    #[tokio::test]
    async fn test_db() {
        let db = Db::memory().await;
        let entity = TestEntity {
            id: 1,
            name: "test".to_string(),
        };
        let get_empty = db.get::<TestEntity>("test/abc").await;
        db.put("test/abc", &entity)
            .await
            .expect("Failed to put entity");
        let get_after_put = db.get::<TestEntity>("test/abc").await;
        let list_after_append = db.list_objects::<TestEntity>("test").await;
        db.delete("test/abc")
            .await
            .expect("Failed to delete entity");
        let get_after_delete = db.get::<TestEntity>("test/abc").await;
        let list_after_remove = db.list_objects::<TestEntity>("test").await;

        insta::assert_debug_snapshot!((
            get_empty,
            get_after_put,
            get_after_delete,
            list_after_append,
            list_after_remove
        ));
    }
}
