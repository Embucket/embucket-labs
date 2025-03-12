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

use crate::QueryHistoryItem;
use async_trait::async_trait;
use bytes::Bytes;
use icebucket_utils::iterable::IterableEntity;
use icebucket_utils::Db;
use snafu::prelude::*;
#[cfg(test)]
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum HistoryStoreError {
    #[snafu(display("Error adding query history: {source}"))]
    QHistoryAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QHistoryGet { source: icebucket_utils::Error },
}

pub type HistoryStoreResult<T> = std::result::Result<T, HistoryStoreError>;

#[async_trait]
pub trait HistoryStore: std::fmt::Debug + Send + Sync {
    async fn add_history_item(&self, item: QueryHistoryItem) -> HistoryStoreResult<()>;
    async fn query_history(
        &self,
        cursor: Option<String>,
        limit: Option<u16>,
    ) -> HistoryStoreResult<Vec<QueryHistoryItem>>;
}

pub struct SlateDBHistoryStore {
    db: Db,
}

impl std::fmt::Debug for SlateDBHistoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBHistoryStore").finish()
    }
}

impl SlateDBHistoryStore {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    #[cfg(test)]
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
}

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    async fn add_history_item(&self, item: QueryHistoryItem) -> HistoryStoreResult<()> {
        Ok(self
            .db
            .put_iterable_entity(&item)
            .await
            .context(QHistoryAddSnafu)?)
    }

    async fn query_history(
        &self,
        cursor: Option<String>,
        limit: Option<u16>,
    ) -> HistoryStoreResult<Vec<QueryHistoryItem>> {
        let start_key = if let Some(cursor) = cursor {
            QueryHistoryItem::key_from_cursor(Bytes::from(cursor))
        } else {
            QueryHistoryItem::min_key()
        };
        let end_key = QueryHistoryItem::max_key();
        Ok(self
            .db
            .items_from_range(start_key..end_key, limit)
            .await
            .context(QHistoryGetSnafu)?)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use icebucket_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    #[tokio::test]
    async fn test_history() {
        let db = SlateDBHistoryStore::new_in_memory().await;
        let n: u16 = 2;
        let mut created: Vec<QueryHistoryItem> = vec![];
        for i in 0..n {
            let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                + Duration::milliseconds(i.into());
            let mut item = QueryHistoryItem::query_start(
                format!("select {i}").as_str(),
                None,
                Some(start_time),
            );
            if i == 0 {
                item.query_finished(1, Some(item.start_time))
            } else {
                item.query_finished_with_error("Test query pseudo error".to_string());
            }
            created.push(item.clone());
            println!("added {:?}", item.key());
            db.add_history_item(item).await.unwrap();
        }

        let cursor = <QueryHistoryItem as IterableEntity>::Cursor::CURSOR_MIN.to_string();
        println!("cursor: {cursor}");
        let retrieved = db.query_history(Some(cursor), Some(10)).await.unwrap();
        for i in 0..retrieved.len() {
            println!("retrieved: {:?}", retrieved[i].key());
        }

        assert_eq!(n as usize, retrieved.len());
        assert_eq!(created, retrieved);
    }
}
