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
use serde::{de::DeserializeOwned, Serialize};
use serde_json::de;
use serde_json::ser;
use slatedb::db::Db as SlateDb;
use slatedb::db_iter::DbIterator;
use slatedb::error::SlateDBError;
use snafu::prelude::*;
use std::sync::Arc;
use std::string::ToString;
use uuid::Uuid;
use bytes::{Bytes, BytesMut};
use std::ops::RangeBounds;

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
}

pub trait IterableEntity {
    const SUFFIX_MAX_LEN: usize;
    const PREFIX: &[u8];
    
    fn key(&self) -> Bytes;
    fn min_key() -> Bytes;
    fn max_key() -> Bytes;

    fn concat_with_prefix<T: ToString>(key_part: T) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::PREFIX.len() + Self::SUFFIX_MAX_LEN);
        buf.extend_from_slice(Self::PREFIX);
        buf.extend_from_slice(key_part.to_string().as_bytes());
        buf.into()
    }
}

// Kind of cast for range, for cases when for range 
// trait `RangeBounds<bytes::Bytes>` is not implemented.
macro_rules! RangeAsRef {
    { $range: ident } => {
        (
            $range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref())),
            $range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()))
        )
    }
}

// To be used with the RangeFull
#[allow(unused_macros)]
macro_rules! RangeFull {
    { $range: ident } => {
        (
            $range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b)),
            $range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b))
        )
    }
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
    pub async fn list_keys(&self, key: &str) -> Result<Vec<String>> {
        let keys: Option<Vec<String>> = self.get(key).await?;
        Ok(keys.unwrap_or_default())
    }

    /// Appends a value to a list stored in the database.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the database operations fail, or
    /// `SerializeError`/`DeserializeError` if the value cannot be serialized or deserialized.
    pub async fn list_append(&self, key: &str, value: String) -> Result<()> {
        self.modify(key, |all_keys: &mut Vec<String>| {
            if !all_keys.contains(&value) {
                all_keys.push(value.clone());
            }
        })
        .await
    }

    /// Removes a value from a list stored in the database.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the database operations fail, or
    /// `SerializeError`/`DeserializeError` if the value cannot be serialized or deserialized.
    pub async fn list_remove(&self, key: &str, value: &str) -> Result<()> {
        self.modify(key, |all_keys: &mut Vec<String>| {
            all_keys.retain(|key| *key != value);
        })
        .await
    }

    /// Modifies a value in the database using the provided closure.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the database operations fail, or
    /// `SerializeError`/`DeserializeError` if the value cannot be serialized or deserialized.
    pub async fn modify<T>(&self, key: &str, f: impl Fn(&mut T) + Send) -> Result<()>
    where
        T: serde::Serialize + DeserializeOwned + Default + Sync + Send,
    {
        let mut value: T = self.get(key).await?.unwrap_or_default();

        f(&mut value);

        self.put(key, &value).await
    }

    /// Stores template object in the database.
    ///
    /// # Errors
    ///
    /// Returns a `SerializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn put_iterable_entity<T: serde::Serialize + Sync + IterableEntity>(&self, entity: &T) -> Result<()>
    {
        let serialized = ser::to_vec(entity).context(SerializeValueSnafu)?;
        self.0.put(entity.key().iter().as_slice(), serialized.as_ref()).await.context(DatabaseSnafu)
    }

   /// Iterator for iterating in range
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn range_iterator<K, T>(&self, range: T) -> Result<DbIterator<'_>>
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        self.0.scan(RangeAsRef!(range)).await.context(DatabaseSnafu)
    }

    /// Fetch iterable items from database
    ///
    /// # Errors
    ///
    /// Returns a `DeserializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.    
    pub async fn items_from_range<K, R, T: for<'de> serde::de::Deserialize<'de> + Sync + IterableEntity>(&self, range: R, limit: Option<u16>) -> Result<Vec<T>> 
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let mut iter = self.range_iterator(range).await?;
        let mut items: Vec<T> = vec![];
        while let Ok(Some(item)) = iter.next().await {
            let item = de::from_slice(&item.value).context(DeserializeValueSnafu)?;
            items.push(item);
            if items.len() >= limit.unwrap_or(u16::MAX).into() {
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
        let key = format!("{}.{}", Self::prefix(), entity.id());
        self.db().put(&key, &entity).await?;
        self.db().list_append(Self::collection_key(), key).await?;
        Ok(())
    }

    async fn _get(&self, id: Uuid) -> Result<Self::Entity> {
        let key = format!("{}.{}", Self::prefix(), id);
        let entity = self.db().get(&key).await?;
        let entity = entity.ok_or(Error::KeyNotFound)?;
        Ok(entity)
    }

    async fn _delete(&self, id: Uuid) -> Result<()> {
        let key = format!("{}.{}", Self::prefix(), id);
        self.db().delete(&key).await?;
        self.db().list_remove(Self::collection_key(), &key).await?;
        Ok(())
    }

    async fn _list(&self) -> Result<Vec<Self::Entity>> {
        let keys = self.db().list_keys(Self::collection_key()).await?;
        let futures = keys
            .iter()
            .map(|key| self.db().get(key))
            .collect::<Vec<_>>();
        let results = futures::future::try_join_all(futures).await?;
        let entities = results.into_iter().flatten().collect::<Vec<Self::Entity>>();
        Ok(entities)
    }

    fn prefix() -> &'static str;
    fn collection_key() -> &'static str;
}


#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
mod test {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use tokio;
    use bytes::{Bytes};
    use std::time::SystemTime;
    use futures::future::join_all;
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
        let get_empty = db.get::<TestEntity>("test").await;
        db.put("test", &entity).await.expect("Failed to put entity");
        let get_after_put = db.get::<TestEntity>("test").await;
        db.delete("test").await.expect("Failed to delete entity");
        let get_after_delete = db.get::<TestEntity>("test").await;

        db.list_append("test_list", "test".to_string())
            .await
            .expect("Failed to append to list");
        let list_after_append = db.list_keys("test_list").await;
        db.list_remove("test_list", "test")
            .await
            .expect("Failed to remove from list");
        let list_after_remove = db.list_keys("test_list").await;

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

    impl IterableEntity for PseudoItem {
        const SUFFIX_MAX_LEN: usize = 19; //for int64::MAX
        const PREFIX: &[u8] = b"hi.";

        fn key(&self) -> Bytes {
            Self::concat_with_prefix(self.start_time.timestamp_nanos_opt().unwrap_or(0))
        }

        fn min_key() -> Bytes {
            Self::concat_with_prefix(0)
        }

        fn max_key() -> Bytes {
            Self::concat_with_prefix(i64::MAX)
        }
    }


    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct PseudoItem2 {
        pub query: String,
        pub start_time: DateTime<Utc>,
    }

    impl IterableEntity for PseudoItem2 {
        const SUFFIX_MAX_LEN: usize = 19; //for int64::MAX
        const PREFIX: &[u8] = b"si.";
        
        fn key(&self) -> Bytes {
            Self::concat_with_prefix(self.start_time.timestamp_nanos_opt().unwrap_or(0))
        }

        fn min_key() -> Bytes {
            Self::concat_with_prefix(0)
        }

        fn max_key() -> Bytes {
            Self::concat_with_prefix(i64::MAX)
        }
    }

    fn new_pseudo_item(prev: Option<PseudoItem>) -> PseudoItem {
        let ts = match prev {
            Some(item) => item.start_time.timestamp(),
            _ => {
                Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap().timestamp()
            }
        };
        let start_time = DateTime::from_timestamp(ts+60*60*24, 0).unwrap();
        PseudoItem {
            query: String::from(format!("SELECT {start_time}")),
            start_time: start_time,
        }
    }

    async fn populate_with_items(db: &Db) -> Vec<PseudoItem> {
        let mut item: Option<PseudoItem> = None;
        
        let started = SystemTime::now();
        println!("Create items {:?}", SystemTime::now().duration_since(started));

        const COUNT: usize = 100;
        let mut items: Vec<PseudoItem> = vec![];
        for _ in 0..COUNT {
            item = Some(new_pseudo_item(item));
            items.push(item.clone().unwrap());
        }
        println!("{} items created {:?}", COUNT, SystemTime::now().duration_since(started));

        let mut fut = Vec::new();
        for item in items.iter() {
            fut.push(db.put_iterable_entity(item))
        }
        join_all(fut).await;
        println!(
            "Added items count={} in {:?}", 
            COUNT, SystemTime::now().duration_since(started)
        );

        let mut iter =  db.0.scan(..).await.unwrap();
        let mut i = 0;
        while let Ok(Some(item)) = iter.next().await {
            assert_eq!(
                item.key,
                Bytes::from(items[i].key())
            );
            assert_eq!(
                item.value,
                Bytes::from(ser::to_string(&items[i]).context(SerializeValueSnafu).unwrap())
            );
            i += 1;
        }
        assert_eq!(i, items.len());
        items
    }

    async fn populate_with_more_items(db: &Db) -> Vec<PseudoItem2>{
        let ts = Utc::now().timestamp();
        let items = vec![
            PseudoItem2{
                query: "SELECT 1".to_string(),
                start_time: DateTime::from_timestamp(ts, 0).unwrap(),
            },
            PseudoItem2{
                query: "SELECT 2".to_string(),
                start_time: DateTime::from_timestamp(ts, 1).unwrap(),
            }
        ];
        for item in items.iter() {
            let _res = db.put_iterable_entity(item).await;
        }
        items
    }

    fn assert_check_items<T: serde::Serialize + Sync + IterableEntity>(created_items: Vec<&T>, retrieved_items: Vec<&T>) {
        assert_eq!(created_items.len(), retrieved_items.len());
        assert_eq!(
            created_items.last().unwrap().key(),
            retrieved_items.last().unwrap().key(),
        );
        for (i, item) in created_items.iter().enumerate() {
            assert_eq!(
                Bytes::from(ser::to_string(&item).context(SerializeValueSnafu).unwrap()),
                Bytes::from(ser::to_string(&retrieved_items[i]).context(SerializeValueSnafu).unwrap()),
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
        println!("PseudoItem range {range:?}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created.iter().collect(), retrieved.iter().collect());

        let created = created_more_items;
        let range = created.first().unwrap().key()..=created.last().unwrap().key();
        println!("PseudoItem2 range {range:?}");
        let retrieved: Vec<PseudoItem2> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created.iter().collect(), retrieved.iter().collect());
    }

    #[tokio::test]
    // test key groups having different prefixes
    async fn test_slatedb_separate_key_groups_within_min_max_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created_more_items = populate_with_more_items(&db).await;

        let range = PseudoItem::min_key()..PseudoItem::max_key();
        println!("PseudoItem range {range:?}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created_items.iter().collect(), retrieved.iter().collect());
        
        let range = PseudoItem2::min_key()..PseudoItem2::max_key();
        println!("PseudoItem2 range {range:?}");
        let retrieved: Vec<PseudoItem2> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(created_more_items.iter().collect(), retrieved.iter().collect());
    }

    #[tokio::test]
    // test keys groups having different prefixes for separate ranges
    async fn test_slatedb_limit() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created = created_items;
        let range = created.first().unwrap().key()..=created.last().unwrap().key();
        let limit: usize = 10;
        println!("PseudoItem range {range:?}, limit {limit}");
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, Some(limit as u16)).await.unwrap();
        assert_check_items(created[0..limit].iter().collect(), retrieved.iter().collect());
    }

    #[tokio::test]
    async fn test_slatedb_start_with_existing_key_end_with_max_key_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let items: Vec<&PseudoItem> = created_items[5..].into_iter().collect();
        let range = items.first().unwrap().key()..PseudoItem::max_key();
        let retrieved: Vec<PseudoItem> = db.items_from_range(range, None).await.unwrap();
        assert_check_items(items, retrieved.iter().collect());
    }

    #[tokio::test]
    // test full range .. and how all the items retrieved
    async fn test_slatedb_dont_distinguish_key_groups_within_full_range() {
        let db = Db::memory().await;
        let created_items = populate_with_items(&db).await;
        let created_more_items = populate_with_more_items(&db).await;

        let range = ..;
        let retrieved: Vec<PseudoItem> = db.items_from_range(RangeFull!(range), None).await.unwrap();
        assert_eq!(created_items.len() + created_more_items.len(), retrieved.len());
        assert_ne!(retrieved.first().unwrap().key(), retrieved.last().unwrap().key());
    }
}
