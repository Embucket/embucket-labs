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

use crate::{QueryRecord, QueryRecordId, Worksheet, WorksheetId};
use async_trait::async_trait;
use icebucket_utils::iterable::{IterableCursor, IterableEntity};
use icebucket_utils::Db;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum WorksheetsStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding worksheet: {source}"))]
    WorksheetAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting worksheet: {source}"))]
    WorksheetGet { source: icebucket_utils::Error },

    #[snafu(display("Error getting worksheets: {source}"))]
    WorksheetsList { source: icebucket_utils::Error },

    #[snafu(display("Error deleting worksheet: {source}"))]
    WorksheetDelete { source: icebucket_utils::Error },

    #[snafu(display("Error updating worksheet: {source}"))]
    WorksheetUpdate { source: icebucket_utils::Error },

    #[snafu(display("Error adding query record: {source}"))]
    QueryAdd { source: icebucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QueryGet { source: icebucket_utils::Error },

    #[snafu(display("Can't locate worksheet by key: {message}"))]
    WorksheetNotFound { message: String },
}

pub type WorksheetsStoreResult<T> = std::result::Result<T, WorksheetsStoreError>;

#[async_trait]
pub trait WorksheetsStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<Worksheet>;
    async fn delete_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<()>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<()>;
    async fn get_worksheets(&self) -> WorksheetsStoreResult<Vec<Worksheet>>;

    async fn add_history_item(&self, item: QueryRecord) -> WorksheetsStoreResult<()>;
    async fn query_history(
        &self,
        worksheet_id: WorksheetId,
        cursor: Option<i64>,
        limit: Option<u16>,
    ) -> WorksheetsStoreResult<Vec<QueryRecord>>;
}

pub struct SlateDBWorksheetsStore {
    db: Db,
}

impl std::fmt::Debug for SlateDBWorksheetsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBWorksheetsStore").finish()
    }
}

impl SlateDBWorksheetsStore {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    pub async fn new_in_memory() -> Arc<Self> {
        Arc::new(Self::new(Db::memory().await))
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
}

#[async_trait]
impl WorksheetsStore for SlateDBWorksheetsStore {
    async fn add_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<Worksheet> {
        self.db
            .put_iterable_entity(&worksheet)
            .await
            .context(WorksheetAddSnafu)?;
        Ok(worksheet)
    }

    async fn get_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<Worksheet> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Worksheet::get_key(id);
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        let res: Option<Worksheet> = self.db.get(key_str).await.context(WorksheetGetSnafu)?;
        res.ok_or_else(|| WorksheetsStoreError::WorksheetNotFound {
            message: key_str.to_string(),
        })
    }

    async fn update_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<()> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = worksheet.key();
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        Ok(self
            .db
            .put(key_str, &worksheet)
            .await
            .context(WorksheetUpdateSnafu)?)
    }

    async fn delete_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<()> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Worksheet::get_key(id);
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(BadKeySnafu)?;

        // raise error if can't locate
        self.get_worksheet(id).await?;

        Ok(self
            .db
            .delete(key_str)
            .await
            .context(WorksheetDeleteSnafu)?)
    }

    async fn get_worksheets(&self) -> WorksheetsStoreResult<Vec<Worksheet>> {
        let start_key = Worksheet::get_key(WorksheetId::CURSOR_MIN);
        let end_key = Worksheet::get_key(WorksheetId::CURSOR_MAX);
        Ok(self
            .db
            .items_from_range(start_key..end_key, None)
            .await
            .context(WorksheetsListSnafu)?)
    }

    async fn add_history_item(&self, item: QueryRecord) -> WorksheetsStoreResult<()> {
        Ok(self
            .db
            .put_iterable_entity(&item)
            .await
            .context(QueryAddSnafu)?)
    }

    async fn query_history(
        &self,
        worksheet_id: WorksheetId,
        cursor: Option<i64>,
        limit: Option<u16>,
    ) -> WorksheetsStoreResult<Vec<QueryRecord>> {
        // TODO: add worksheet_id to key prefix
        let start_key = if let Some(cursor) = cursor {
            QueryRecord::get_key(worksheet_id, cursor)
        } else {
            QueryRecord::get_key(worksheet_id, QueryRecordId::CURSOR_MIN)
        };
        let end_key = QueryRecord::get_key(worksheet_id, QueryRecordId::CURSOR_MAX);
        Ok(self
            .db
            .items_from_range(start_key..end_key, limit)
            .await
            .context(QueryGetSnafu)?)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use icebucket_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    #[tokio::test]
    async fn test_history() {
        let db = SlateDBWorksheetsStore::new_in_memory().await;

        // create worksheet first
        let worksheet = Worksheet::new(String::new(), String::new());

        let n: u16 = 2;
        let mut created: Vec<QueryRecord> = vec![];
        for i in 0..n {
            let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                + Duration::milliseconds(i.into());
            let mut item = QueryRecord::query_start(
                worksheet.id,
                format!("select {i}").as_str(),
                Some(start_time),
            );
            if i == 0 {
                item.query_finished(
                    1,
                    Some(String::from("pseudo result")),
                    Some(item.start_time),
                );
            } else {
                item.query_finished_with_error("Test query pseudo error".to_string());
            }
            created.push(item.clone());
            eprintln!("added {:?}", item.key());
            db.add_history_item(item).await.unwrap();
        }

        let cursor = <QueryRecord as IterableEntity>::Cursor::CURSOR_MIN;
        eprintln!("cursor: {cursor}");
        let retrieved = db
            .query_history(worksheet.id, Some(cursor), Some(10))
            .await
            .unwrap();
        for item in &retrieved {
            eprintln!("retrieved: {:?}", item.key());
        }

        assert_eq!(usize::from(n), retrieved.len());
        assert_eq!(created, retrieved);
    }
}
