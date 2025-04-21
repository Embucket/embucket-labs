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

pub mod iterable;
pub mod scan_iterator;

use crate::scan_iterator::{ScanIterator, VecScanIterator};
use async_trait::async_trait;
use bytes::Bytes;
use iterable::IterableEntity;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::de;
use serde_json::ser;
use slatedb::db::Db as SlateDb;
use slatedb::db_iter::DbIterator;
use slatedb::error::SlateDBError;
use snafu::prelude::*;
use std::ops::RangeBounds;
use std::string::ToString;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
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

#[derive(Clone)]
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

    // /// Retrieves a list of objects from the database.
    // ///
    // /// # Errors
    // ///
    // /// Returns a `DbError` if the underlying database operation fails.
    // /// Returns a `DeserializeError` if the value cannot be deserialized from JSON.
    // #[allow(clippy::unwrap_used)]
    // pub async fn list_objects<T: Send + for<'de> serde::de::Deserialize<'de>>(
    //     &self,
    //     key: &str,
    //     list_config: ListConfig,
    // ) -> Result<Vec<T>> {
    //     //We can look with respect to limit
    //     // from start to end (full scan),
    //     // from starts_with to start_with (search),
    //     // from cursor to end (looking not from the start)
    //     // and from cursor to prefix (search without starting at the start and looking to the end (no full scan))
    //     // more info in `list_config` file
    //     let start = list_config.token.clone().map_or_else(
    //         || format!("{key}/"),
    //         |search_prefix| format!("{key}/{search_prefix}"),
    //     );
    //     let start = list_config
    //         .cursor
    //         .map_or_else(|| start, |cursor| format!("{key}/{cursor}\x00"));
    //     let end = list_config.token.map_or_else(
    //         || format!("{key}/\x7F"),
    //         |search_prefix| format!("{key}/{search_prefix}\x7F"),
    //     );
    //     let range = Bytes::from(start)..Bytes::from(end);
    //     let limit = list_config.limit.unwrap_or(usize::MAX);
    //     let mut iter = self.0.scan(range).await.context(ScanFailedSnafu)?;
    //     let mut objects: Vec<T> = vec![];
    //     while let Ok(Some(value)) = iter.next().await {
    //         let value = de::from_slice(&value.value).context(DeserializeValueSnafu)?;
    //         objects.push(value);
    //         if objects.len() >= limit {
    //             break;
    //         }
    //     }
    //     Ok(objects)
    // }

    #[must_use]
    pub fn iter_objects<T: Send + for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: String,
    ) -> VecScanIterator<T> {
        VecScanIterator::new(self.0.clone(), key)
    }

    /// Stores template object in the database.
    ///
    /// # Errors
    ///
    /// Returns a `SerializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn put_iterable_entity<T: serde::Serialize + Sync + IterableEntity>(
        &self,
        entity: &T,
    ) -> Result<()> {
        let serialized = ser::to_vec(entity).context(SerializeValueSnafu)?;
        self.0
            .put(entity.key().as_ref(), serialized.as_ref())
            .await
            .context(DatabaseSnafu)
    }

    /// Iterator for iterating in range
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn range_iterator<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
    ) -> Result<DbIterator<'_>> {
        self.0.scan(range).await.context(DatabaseSnafu)
    }

    /// Fetch iterable items from database
    ///
    /// # Errors
    ///
    /// Returns a `DeserializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.    
    pub async fn items_from_range<
        R: RangeBounds<Bytes> + Send,
        T: for<'de> serde::de::Deserialize<'de> + IterableEntity + Sync + Send,
    >(
        &self,
        range: R,
        limit: Option<u16>,
    ) -> Result<Vec<T>> {
        let mut iter = self.range_iterator(range).await?;
        let mut items: Vec<T> = vec![];
        while let Ok(Some(item)) = iter.next().await {
            let item = de::from_slice(&item.value).context(DeserializeValueSnafu)?;
            items.push(item);
            if items.len() >= usize::from(limit.unwrap_or(u16::MAX)) {
                break;
            }
        }
        Ok(items)
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
        let entities = self
            .db()
            .iter_objects(Self::collection_key())
            .collect()
            .await?;
        Ok(entities)
    }

    fn prefix() -> &'static str;
    fn collection_key() -> String;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
mod test {
    use super::*;
    use bytes::Bytes;
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use futures::future::join_all;
    use iterable::IterableEntity;
    use serde::{Deserialize, Serialize};
    use std::time::SystemTime;

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
        let list_after_append = db
            .iter_objects::<TestEntity>("test".to_string())
            .collect()
            .await;
        db.delete("test/abc")
            .await
            .expect("Failed to delete entity");
        let get_after_delete = db.get::<TestEntity>("test/abc").await;
        let list_after_remove = db
            .iter_objects::<TestEntity>("test".to_string())
            .collect()
            .await;

        insta::assert_debug_snapshot!((
            get_empty,
            get_after_put,
            get_after_delete,
            list_after_append,
            list_after_remove
        ));
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct PseudoItem {
        pub query: String,
        pub start_time: DateTime<Utc>,
    }

    impl PseudoItem {
        pub fn get_key(id: i64) -> Bytes {
            Bytes::from(format!("hi.{id}"))
        }
    }

    impl IterableEntity for PseudoItem {
        type Cursor = i64;

        fn cursor(&self) -> Self::Cursor {
            self.start_time.timestamp_millis()
        }

        fn key(&self) -> Bytes {
            Self::get_key(self.cursor())
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct PseudoItem2 {
        pub query: String,
        pub start_time: DateTime<Utc>,
    }

    impl PseudoItem2 {
        pub fn get_key(id: i64) -> Bytes {
            Bytes::from(format!("si.{id}"))
        }
    }

    impl IterableEntity for PseudoItem2 {
        type Cursor = i64;

        fn cursor(&self) -> Self::Cursor {
            self.start_time.timestamp_millis()
        }

        fn key(&self) -> Bytes {
            Self::get_key(self.cursor())
        }
    }

    fn new_pseudo_item(prev: Option<PseudoItem>) -> PseudoItem {
        let start_time = match prev {
            Some(item) => item.start_time,
            _ => Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
        };
        let start_time = start_time + Duration::days(1);
        PseudoItem {
            query: format!("SELECT {start_time}"),
            start_time,
        }
    }

    #[allow(clippy::items_after_statements)]
    async fn populate_with_items(db: &Db) -> Vec<PseudoItem> {
        let mut item: Option<PseudoItem> = None;

        let started = SystemTime::now();
        eprintln!(
            "Create items {:?}",
            SystemTime::now().duration_since(started)
        );

        const COUNT: usize = 100;
        let mut items: Vec<PseudoItem> = vec![];
        for _ in 0..COUNT {
            item = Some(new_pseudo_item(item));
            items.push(item.clone().unwrap());
        }
        eprintln!(
            "{} items created {:?}",
            COUNT,
            SystemTime::now().duration_since(started)
        );

        let mut fut = Vec::new();
        for item in &items {
            eprintln!("Add item, key={:?}", item.key());
            fut.push(db.put_iterable_entity(item));
        }
        join_all(fut).await;
        eprintln!(
            "Added items count={} in {:?}",
            COUNT,
            SystemTime::now().duration_since(started)
        );

        let mut iter = db.0.scan(..).await.unwrap();
        let mut i = 0;
        while let Ok(Some(item)) = iter.next().await {
            assert_eq!(item.key, items[i].key());
            assert_eq!(
                item.value,
                Bytes::from(
                    ser::to_string(&items[i])
                        .context(SerializeValueSnafu)
                        .unwrap()
                )
            );
            i += 1;
        }
        assert_eq!(i, items.len());
        items
    }

    async fn populate_with_more_items(db: &Db) -> Vec<PseudoItem2> {
        let start_time = Utc::now();
        let items = vec![
            PseudoItem2 {
                query: "SELECT 1".to_string(),
                start_time,
            },
            PseudoItem2 {
                query: "SELECT 2".to_string(),
                start_time: start_time + Duration::milliseconds(1),
            },
        ];
        for item in &items {
            let _res = db.put_iterable_entity(item).await;
        }
        items
    }

    fn assert_check_items<T: serde::Serialize + Sync + IterableEntity>(
        created_items: &[T],
        retrieved_items: &[T],
    ) {
        assert_eq!(created_items.len(), retrieved_items.len());
        assert_eq!(
            created_items.last().unwrap().key(),
            retrieved_items.last().unwrap().key(),
        );
        for (i, item) in created_items.iter().enumerate() {
            assert_eq!(
                Bytes::from(ser::to_string(&item).context(SerializeValueSnafu).unwrap()),
                Bytes::from(
                    ser::to_string(&retrieved_items[i])
                        .context(SerializeValueSnafu)
                        .unwrap()
                ),
            );
        }
    }

    #[tokio::test]
    // test keys groups having different prefixes for separate ranges
    async fn test_slatedb_separate_keys_groups() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created_more_items = populate_with_more_items(&db).await;

        let created = created_items;
        let range = created.first().unwrap().key()..=created.last().unwrap().key();
        eprintln!("PseudoItem range {range:?}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created.as_slice(), retrieved.as_slice());

        let created = created_more_items;
        let range = created.first().unwrap().key()..=created.last().unwrap().key();
        eprintln!("PseudoItem2 range {range:?}");
        let retrieved: Vec<PseudoItem2> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created.as_slice(), retrieved.as_slice());
    }

    #[tokio::test]
    // test key groups having different prefixes
    async fn test_slatedb_separate_key_groups_within_min_max_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created_more_items = populate_with_more_items(&db).await;

        let range = PseudoItem::get_key(PseudoItem::min_cursor())
            ..PseudoItem::get_key(PseudoItem::max_cursor());
        eprintln!("PseudoItem range {range:?}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created_items.as_slice(), retrieved.as_slice());

        let range = PseudoItem2::get_key(PseudoItem2::min_cursor())
            ..PseudoItem2::get_key(PseudoItem2::max_cursor());
        eprintln!("PseudoItem2 range {range:?}");
        let retrieved: Vec<PseudoItem2> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created_more_items.as_slice(), retrieved.as_slice());
    }

    #[tokio::test]
    // test keys groups having different prefixes for separate ranges
    async fn test_slatedb_limit() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created = created_items;
        let range = created.first().unwrap().key()..=created.last().unwrap().key();
        let limit: u16 = 10;
        eprintln!("PseudoItem range {range:?}, limit {limit}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, Some(limit)).await.unwrap();
        assert_check_items(
            created[0..limit.into()].iter().as_slice(),
            retrieved.as_slice(),
        );
    }

    #[tokio::test]
    async fn test_slatedb_start_with_existing_key_end_with_max_key_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let items = created_items[5..].iter().as_slice();
        let range = items.first().unwrap().key()..PseudoItem::get_key(PseudoItem::max_cursor());
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(items, retrieved.as_slice());
    }

    #[tokio::test]
    // test full range .. and how all the items retrieved
    async fn test_slatedb_dont_distinguish_key_groups_within_full_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created_more_items = populate_with_more_items(&db).await;

        let range = ..;
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_eq!(
            created_items.len() + created_more_items.len(),
            retrieved.len()
        );
        assert_ne!(
            retrieved.first().unwrap().key(),
            retrieved.last().unwrap().key()
        );
    }
}
