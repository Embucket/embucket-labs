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

use crate::{QueryRecord, QueryRecordId, QueryRecordReference, Worksheet, WorksheetId};
use async_trait::async_trait;
use embucket_utils::iterable::{IterableCursor, IterableEntity};
use embucket_utils::{Db, Error};
use serde_json::de;
use slatedb::error::SlateDBError;
use snafu::{ResultExt, Snafu};
use std::str;
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum WorksheetsStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding worksheet: {source}"))]
    WorksheetAdd { source: embucket_utils::Error },

    #[snafu(display("Error getting worksheet: {source}"))]
    WorksheetGet { source: embucket_utils::Error },

    #[snafu(display("Error getting worksheets: {source}"))]
    WorksheetsList { source: embucket_utils::Error },

    #[snafu(display("Error deleting worksheet: {source}"))]
    WorksheetDelete { source: embucket_utils::Error },

    #[snafu(display("Error updating worksheet: {source}"))]
    WorksheetUpdate { source: embucket_utils::Error },

    #[snafu(display("Error adding query record: {source}"))]
    QueryAdd { source: embucket_utils::Error },

    #[snafu(display("Error adding query record reference: {source}"))]
    QueryReferenceAdd { source: embucket_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QueryGet { source: embucket_utils::Error },

    #[snafu(display("Can't locate worksheet by key: {message}"))]
    WorksheetNotFound { message: String },

    #[snafu(display("Bad query record reference key: {key}"))]
    QueryReferenceKey { key: String },

    #[snafu(display("Error getting worksheet queries: {source}"))]
    GetWorksheetQueries { source: Error },

    #[snafu(display("Query item seek error: {source}"))]
    Seek { source: SlateDBError },

    #[snafu(display("Deserialize error: {source}"))]
    DeserializeValue { source: serde_json::Error },
}

pub type WorksheetsStoreResult<T> = std::result::Result<T, WorksheetsStoreError>;

#[async_trait]
pub trait WorksheetsStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<Worksheet>;
    async fn delete_worksheet(&self, id: WorksheetId) -> WorksheetsStoreResult<()>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> WorksheetsStoreResult<()>;
    async fn get_worksheets(&self) -> WorksheetsStoreResult<Vec<Worksheet>>;

    async fn add_query(&self, item: &QueryRecord) -> WorksheetsStoreResult<()>;
    async fn get_queries(
        &self,
        worksheet_id: Option<WorksheetId>,
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

    async fn add_query(&self, item: &QueryRecord) -> WorksheetsStoreResult<()> {
        if let Some(worksheet_id) = item.worksheet_id {
            // add query reference to worksheet
            self.db
                .put_iterable_entity(&QueryRecordReference {
                    id: item.id,
                    worksheet_id,
                })
                .await
                .context(QueryReferenceAddSnafu)?;
        }

        // add query record
        Ok(self
            .db
            .put_iterable_entity(item)
            .await
            .context(QueryAddSnafu)?)
    }

    async fn get_queries(
        &self,
        worksheet_id: Option<WorksheetId>,
        cursor: Option<i64>,
        limit: Option<u16>,
    ) -> WorksheetsStoreResult<Vec<QueryRecord>> {
        if let Some(worksheet_id) = worksheet_id {
            let start_key = QueryRecord::get_key(cursor.unwrap_or(QueryRecordId::CURSOR_MIN));
            let end_key = QueryRecord::get_key(QueryRecordId::CURSOR_MAX);
            let mut iter = self
                .db
                .range_iterator(start_key..end_key)
                .await
                .context(GetWorksheetQueriesSnafu)?;

            // get queries references related to worksheet
            let refs_start_key = QueryRecordReference::get_key(
                worksheet_id,
                cursor.unwrap_or(QueryRecordId::CURSOR_MIN),
            );
            let refs_end_key =
                QueryRecordReference::get_key(worksheet_id, QueryRecordId::CURSOR_MAX);
            let mut refs_iter = self
                .db
                .range_iterator(refs_start_key..refs_end_key)
                .await
                .context(GetWorksheetQueriesSnafu)?;

            let mut items: Vec<QueryRecord> = vec![];
            while let Ok(Some(item)) = refs_iter.next().await {
                let qh_key = QueryRecordReference::extract_qh_key(&item.key).ok_or(
                    WorksheetsStoreError::QueryReferenceKey {
                        key: format!("{:?}", item.key),
                    },
                )?;
                iter.seek(qh_key).await.context(SeekSnafu)?;
                match iter.next().await {
                    Ok(Some(query_record_kv)) => {
                        items.push(
                            de::from_slice(&query_record_kv.value)
                                .context(DeserializeValueSnafu)?,
                        );
                        if items.len() >= usize::from(limit.unwrap_or(u16::MAX)) {
                            break;
                        }
                    }
                    _ => break,
                };
            }
            Ok(items)
        } else {
            let start_key = QueryRecord::get_key(cursor.unwrap_or(QueryRecordId::CURSOR_MIN));
            let end_key = QueryRecord::get_key(QueryRecordId::CURSOR_MAX);
            Ok(self
                .db
                .items_from_range(start_key..end_key, limit)
                .await
                .context(QueryGetSnafu)?)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::*;
    use chrono::{Duration, TimeZone, Utc};
    use embucket_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    fn create_query_records(templates: &[(Option<i64>, QueryStatus)]) -> Vec<QueryRecord> {
        let mut created: Vec<QueryRecord> = vec![];
        for (i, (worksheet_id, query_status)) in templates.iter().enumerate() {
            let ctx = MockQueryRecordActions::query_start_context();
            ctx.expect().returning(move |query, worksheet_id| {
                let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                    + Duration::milliseconds(i.try_into().unwrap());
                QueryRecord {
                    id: start_time.timestamp_millis(),
                    worksheet_id,
                    query: query.to_string(),
                    start_time,
                    end_time: start_time,
                    duration_ms: 0,
                    result_count: 0,
                    result: None,
                    status: QueryStatus::Running,
                    error: None,
                }
            });
            let query_record = match query_status {
                QueryStatus::Running => MockQueryRecordActions::query_start(
                    format!("select {i}").as_str(),
                    *worksheet_id,
                ),
                QueryStatus::Successful => {
                    let mut item = MockQueryRecordActions::query_start(
                        format!("select {i}").as_str(),
                        *worksheet_id,
                    );
                    item.query_finished(1, Some(String::from("pseudo result")));
                    item
                }
                QueryStatus::Failed => {
                    let mut item = MockQueryRecordActions::query_start(
                        format!("select {i}").as_str(),
                        *worksheet_id,
                    );
                    item.query_finished_with_error(String::from("Test query pseudo error"));
                    item
                }
            };
            created.push(query_record);
        }

        created
    }

    #[tokio::test]
    async fn test_history() {
        let db = SlateDBWorksheetsStore::new_in_memory().await;

        // create worksheet first
        let worksheet = Worksheet::new(String::new(), String::new());

        let created = create_query_records(&[
            (Some(worksheet.id), QueryStatus::Successful),
            (Some(worksheet.id), QueryStatus::Failed),
            (Some(worksheet.id), QueryStatus::Running),
            (None, QueryStatus::Running),
        ]);

        for item in &created {
            eprintln!("added {:?}", item.key());
            db.add_query(item).await.unwrap();
        }

        let cursor = <QueryRecord as IterableEntity>::Cursor::CURSOR_MIN;
        eprintln!("cursor: {cursor}");
        let retrieved = db
            .get_queries(Some(worksheet.id), Some(cursor), Some(10))
            .await
            .unwrap();
        // queries belong to worksheet
        assert_eq!(3, retrieved.len());

        let retrieved_all = db.get_queries(None, Some(cursor), Some(10)).await.unwrap();
        // all queries
        for item in &retrieved_all {
            eprintln!("retrieved_all: {:?}", item.key());
        }
        assert_eq!(created.len(), retrieved_all.len());
        assert_eq!(created, retrieved_all);

        // TODO: delete worksheet & check history
    }
}
