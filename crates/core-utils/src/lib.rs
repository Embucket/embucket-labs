pub mod errors;
pub mod iterable;
pub mod scan_iterator;

pub use errors::{Error, Result};

use crate::scan_iterator::{ScanIterator, VecScanIterator};
use async_trait::async_trait;
use bytes::Bytes;
use iterable::IterableEntity;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::de;
use serde_json::ser;
use slatedb::Db as SlateDb;
use slatedb::DbIterator;
// use slatedb::config::{PutOptions, WriteOptions};
use snafu::location;
use snafu::prelude::*;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::string::ToString;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;
use core_sqlite::SqliteStore;

#[derive(Clone)]
pub struct Db{
    pub slatedb: Arc<SlateDb>,
    pub sqlite: Arc<SqliteStore>,
}

impl Db {
    #[allow(clippy::expect_used)]
    pub async fn new(db: Arc<SlateDb>) -> Self {       
        Self {
            slatedb: db.clone(),
            sqlite: Arc::new(SqliteStore::init(db).await
                .expect("Failed to initialize sqlite store")),
        }
    }

    #[allow(clippy::expect_used)]
    pub async fn memory() -> Self {
        let object_store = object_store::memory::InMemory::new();
        let db = Arc::new(SlateDb::open(
            object_store::path::Path::from("/"),
            std::sync::Arc::new(object_store),
        )
        .await
        .expect("Failed to open database"));
        Self{
            slatedb: db.clone(),
            sqlite: Arc::new(SqliteStore::init(db).await
                .expect("Failed to initialize sqlite store")),
        }
    }

    #[must_use]
    pub fn slate_db(&self) -> Arc<SlateDb> {
        self.slatedb.clone()
    }

    #[must_use]
    pub fn sqlite(&self) -> Arc<SqliteStore> {
        self.sqlite.clone()
    }

    /// Closes the database connection.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    pub async fn close(&self) -> Result<()> {
        self.slatedb.close().await.context(errors::DatabaseSnafu)?;
        Ok(())
    }

    /// Deletes a key-value pair from the database.
    ///
    /// # Errors
    ///
    /// This function will return a `DbError` if the underlying database operation fails.
    #[instrument(name = "Db::delete", level = "trace", skip(self), err)]
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.slatedb
            .delete(key.as_bytes())
            .await
            .context(errors::KeyDeleteSnafu {
                key: key.to_string(),
            })
    }

    /// Deletes a key-value pair from the database.
    ///
    /// # Errors
    ///
    /// This function will return a `DbError` if the underlying database operation fails.
    #[instrument(name = "Db::delete_key", level = "trace", skip(self), err)]
    pub async fn delete_key(&self, key: Bytes) -> Result<()> {
        self.slatedb
            .delete(key.as_ref())
            .await
            .context(errors::KeyDeleteSnafu {
                key: format!("{key:?}"),
            })
    }

    /// Stores a key-value pair in the database.
    ///
    /// # Errors
    ///
    /// Returns a `SerializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.
    #[instrument(name = "Db::put", level = "trace", skip(self, value), err)]
    pub async fn put<T: serde::Serialize + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let serialized = ser::to_vec(value).context(errors::SerializeValueSnafu)?;
        self.slatedb
            .put(key.as_bytes(), serialized)
            .await
            .context(errors::KeyPutSnafu {
                key: key.to_string(),
            })
    }

    /// Retrieves a value from the database by its key.
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    /// Returns a `DeserializeError` if the value cannot be deserialized from JSON.
    #[instrument(name = "Db::get", level = "trace", skip(self), err)]
    pub async fn get<T: for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let value: Option<bytes::Bytes> =
            self.slatedb
                .get(key.as_bytes())
                .await
                .context(errors::KeyGetSnafu {
                    key: key.to_string(),
                })?;
        value.map_or_else(
            || Ok(None),
            |bytes| {
                de::from_slice(&bytes).context(errors::DeserializeValueSnafu {
                    key: Bytes::from(key.to_string()),
                })
            },
        )
    }

    #[must_use]
    #[instrument(name = "Db::iter_objects", level = "trace", skip(self))]
    pub fn iter_objects<T: Send + for<'de> serde::de::Deserialize<'de>>(
        &self,
        key: String,
    ) -> VecScanIterator<T> {
        VecScanIterator::new(self.slatedb.clone(), key)
    }

    /// Stores template object in the database. This function primarily used by history store
    /// so we store history objects using quicker but less durable method
    ///
    /// # Errors
    ///
    /// Returns a `SerializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.
    #[instrument(name = "Db::put_iterable_entity", level = "trace", fields(key=format!("{:?}", entity.key())), skip(self, entity), err)]
    pub async fn put_iterable_entity<T: serde::Serialize + Sync + IterableEntity>(
        &self,
        entity: &T,
    ) -> Result<()> {
        let serialized = ser::to_vec(entity).context(errors::SerializeValueSnafu)?;
        self.slatedb
            .put(entity.key().as_ref(), serialized)
            // .put_with_options(
            //     entity.key().as_ref(),
            //     serialized,
            //     &PutOptions::default(),
            //     &WriteOptions {
            //         await_durable: false,
            //     },
            // )
            .await
            .context(errors::DatabaseSnafu)
    }

    /// Iterator for iterating in range
    ///
    /// # Errors
    ///
    /// Returns a `DbError` if the underlying database operation fails.
    #[instrument(name = "Db::range_iterator", level = "trace", skip(self), err)]
    pub async fn range_iterator<R: RangeBounds<Bytes> + Send + Debug>(
        &self,
        range: R,
    ) -> Result<DbIterator<'_>> {
        self.slatedb.scan(range).await.context(errors::DatabaseSnafu)
    }

    /// Fetch iterable items from database
    ///
    /// # Errors
    ///
    /// Returns a `DeserializeError` if the value cannot be serialized to JSON.
    /// Returns a `DbError` if the underlying database operation fails.    
    #[instrument(
        name = "Db::items_from_range",
        level = "trace",
        skip(self),
        fields(items_count),
        err
    )]
    pub async fn items_from_range<
        R: RangeBounds<Bytes> + Send + Debug,
        T: for<'de> serde::de::Deserialize<'de> + IterableEntity + Sync + Send,
    >(
        &self,
        range: R,
        limit: Option<u16>,
    ) -> Result<Vec<T>> {
        let mut iter = self.range_iterator(range).await?;
        let mut items: Vec<T> = vec![];
        while let Ok(Some(item)) = iter.next().await {
            let item = de::from_slice(&item.value)
                .context(errors::DeserializeValueSnafu { key: item.key })?;
            items.push(item);
            if items.len() >= usize::from(limit.unwrap_or(u16::MAX)) {
                break;
            }
        }

        // Record the result as part of the current span.
        tracing::Span::current().record("items_count", items.len());

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
        let entity = entity.ok_or(Error::KeyNotFound {
            location: location!(),
        })?;
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
    use std::ops::Bound;
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

        let full_range: (Bound<Bytes>, Bound<Bytes>) = (Bound::Unbounded, Bound::Unbounded);
        let mut iter = db.slatedb.scan(full_range).await.unwrap();
        let mut i = 0;
        while let Ok(Some(item)) = iter.next().await {
            assert_eq!(item.key, items[i].key());
            assert_eq!(
                item.value,
                Bytes::from(
                    ser::to_string(&items[i])
                        .context(errors::SerializeValueSnafu)
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
                Bytes::from(
                    ser::to_string(&item)
                        .context(errors::SerializeValueSnafu)
                        .unwrap()
                ),
                Bytes::from(
                    ser::to_string(&retrieved_items[i])
                        .context(errors::SerializeValueSnafu)
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
