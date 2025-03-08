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

use crate::HistoryItem;
use async_trait::async_trait;
use bytes::Bytes;
use icebucket_utils::iterable::IterableEntity;
use icebucket_utils::Db;
use snafu::prelude::*;
#[cfg(test)]
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum QHistoryError {
    #[snafu(display("Error adding query history: {source}"))]
    QHistoryAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QHistoryGet { source: icebucket_utils::Error },
}

pub type QHistoryResult<T> = std::result::Result<T, QHistoryError>;

#[async_trait]
pub trait QHistoryApi: Send + Sync {
    async fn add_history_item(&self, item: HistoryItem) -> QHistoryResult<()>;
    async fn query_history(
        &self,
        cursor: Option<String>,
        limit: Option<u16>,
    ) -> QHistoryResult<Vec<HistoryItem>>;
}

pub struct QHistoryStore {
    db: Db,
}

impl QHistoryStore {
    #[must_use]
    pub fn new(db: Db) -> Self {
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
impl QHistoryApi for QHistoryStore {
    async fn add_history_item(&self, item: HistoryItem) -> QHistoryResult<()> {
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
    ) -> QHistoryResult<Vec<HistoryItem>> {
        let start_key = if let Some(cursor) = cursor {
            HistoryItem::key_from_cursor(Bytes::from(cursor))
        } else {
            HistoryItem::min_key()
        };
        let end_key = HistoryItem::max_key();
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
    use chrono::{DateTime, TimeZone, Utc};
    use icebucket_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_history() {
        let db = QHistoryStore::new_in_memory().await;
        let n: u16 = 2;
        let ts: i64 = Utc
            .with_ymd_and_hms(2020, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let mut created: Vec<HistoryItem> = vec![];
        for i in 0..n {
            let item = HistoryItem {
                id: Uuid::new_v4(),
                query: format!("select {i}"),
                start_time: DateTime::from_timestamp(ts, i.into()).unwrap(),
                end_time: DateTime::from_timestamp(ts, i.into()).unwrap(),
                status_code: if i == 0 { 200 } else { 500 },
                error: if i == 0 {
                    None
                } else {
                    Some("Test query pseudo error".to_string())
                },
            };
            created.push(item.clone());
            db.add_history_item(item).await.unwrap();
        }

        let retrieved = db
            .query_history(
                Some(<HistoryItem as IterableEntity>::Cursor::CURSOR_MIN.to_string()),
                Some(10),
            )
            .await
            .unwrap();
        assert_eq!(n as usize, retrieved.len());
        assert_eq!(created, retrieved);
    }
}
