use crate::errors::{self as errors, HistoryStoreError};
use crate::{
    QueryRecord, QueryRecordId, QueryRecordReference, SlateDBHistoryStore, Worksheet, WorksheetId,
};
use async_trait::async_trait;
use core_utils::Db;
use core_utils::iterable::IterableCursor;
use futures::future::join_all;
use serde_json::de;
use slatedb::DbIterator;
use snafu::ResultExt;

pub type HistoryStoreResult<T> = Result<T, HistoryStoreError>;

#[derive(Default, Clone, Debug)]
pub enum SortOrder {
    Ascending,
    #[default]
    Descending,
}

#[derive(Default)]
pub struct GetQueriesParams {
    pub worksheet_id: Option<WorksheetId>,
    pub sql_text: Option<String>,     // filter by SQL Text
    pub min_duration_ms: Option<i64>, // filter Duration greater than
    pub cursor: Option<QueryRecordId>,
    pub limit: Option<u16>,
}

impl GetQueriesParams {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn with_worksheet_id(mut self, worksheet_id: WorksheetId) -> Self {
        self.worksheet_id = Some(worksheet_id);
        self
    }

    #[must_use]
    pub fn with_sql_text(mut self, sql_text: String) -> Self {
        self.sql_text = Some(sql_text);
        self
    }

    #[must_use]
    pub const fn with_min_duration_ms(mut self, min_duration_ms: i64) -> Self {
        self.min_duration_ms = Some(min_duration_ms);
        self
    }

    #[must_use]
    pub const fn with_cursor(mut self, cursor: QueryRecordId) -> Self {
        self.cursor = Some(cursor);
        self
    }

    #[must_use]
    pub const fn with_limit(mut self, limit: u16) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[async_trait]
pub trait HistoryStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> HistoryStoreResult<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> HistoryStoreResult<Worksheet>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> HistoryStoreResult<()>;
    async fn delete_worksheet(&self, id: WorksheetId) -> HistoryStoreResult<()>;
    async fn get_worksheets(&self) -> HistoryStoreResult<Vec<Worksheet>>;
    async fn add_query(&self, item: &QueryRecord) -> HistoryStoreResult<()>;
    async fn get_query(&self, id: QueryRecordId) -> HistoryStoreResult<QueryRecord>;
    async fn get_queries(&self, params: GetQueriesParams) -> HistoryStoreResult<Vec<QueryRecord>>;
}

async fn queries_iterator(
    db: &Db,
    cursor: Option<QueryRecordId>,
) -> HistoryStoreResult<DbIterator<'_>> {
    let start_key = QueryRecord::get_key(cursor.unwrap_or_else(QueryRecordId::min_cursor));
    let end_key = QueryRecord::get_key(QueryRecordId::max_cursor());
    db.range_iterator(start_key..end_key)
        .await
        .context(errors::GetWorksheetQueriesSnafu)
}

async fn worksheet_queries_references_iterator(
    db: &Db,
    worksheet_id: WorksheetId,
    cursor: Option<QueryRecordId>,
) -> HistoryStoreResult<DbIterator<'_>> {
    let refs_start_key = QueryRecordReference::get_key(
        worksheet_id,
        cursor.unwrap_or_else(QueryRecordId::min_cursor),
    );
    let refs_end_key = QueryRecordReference::get_key(worksheet_id, QueryRecordId::max_cursor());
    db.range_iterator(refs_start_key..refs_end_key)
        .await
        .context(errors::GetWorksheetQueriesSnafu)
}

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    async fn add_worksheet(&self, worksheet: Worksheet) -> HistoryStoreResult<Worksheet> {
        self.db
            .put_iterable_entity(&worksheet)
            .await
            .context(errors::WorksheetAddSnafu)?;
        Ok(worksheet)
    }

    async fn get_worksheet(&self, id: WorksheetId) -> HistoryStoreResult<Worksheet> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Worksheet::get_key(id);
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(errors::BadKeySnafu)?;

        let res: Option<Worksheet> = self
            .db
            .get(key_str)
            .await
            .context(errors::WorksheetGetSnafu)?;
        res.ok_or_else(|| HistoryStoreError::WorksheetNotFound {
            message: key_str.to_string(),
        })
    }

    async fn update_worksheet(&self, mut worksheet: Worksheet) -> HistoryStoreResult<()> {
        worksheet.set_updated_at(None);

        Ok(self
            .db
            .put_iterable_entity(&worksheet)
            .await
            .context(errors::WorksheetUpdateSnafu)?)
    }

    async fn delete_worksheet(&self, id: WorksheetId) -> HistoryStoreResult<()> {
        // raise an error if we can't locate
        self.get_worksheet(id).await?;

        let mut ref_iter = worksheet_queries_references_iterator(&self.db, id, None).await?;

        let mut fut = Vec::new();
        while let Ok(Some(item)) = ref_iter.next().await {
            fut.push(self.db.delete_key(item.key));
        }
        join_all(fut).await;

        Ok(self
            .db
            .delete_key(Worksheet::get_key(id))
            .await
            .context(errors::WorksheetDeleteSnafu)?)
    }

    async fn get_worksheets(&self) -> HistoryStoreResult<Vec<Worksheet>> {
        let start_key = Worksheet::get_key(WorksheetId::min_cursor());
        let end_key = Worksheet::get_key(WorksheetId::max_cursor());
        Ok(self
            .db
            .items_from_range(start_key..end_key, None)
            .await
            .context(errors::WorksheetsListSnafu)?)
    }

    async fn add_query(&self, item: &QueryRecord) -> HistoryStoreResult<()> {
        if let Some(worksheet_id) = item.worksheet_id {
            // add query reference to the worksheet
            self.db
                .put_iterable_entity(&QueryRecordReference {
                    id: item.id,
                    worksheet_id,
                })
                .await
                .context(errors::QueryReferenceAddSnafu)?;
        }

        // add query record
        Ok(self
            .db
            .put_iterable_entity(item)
            .await
            .context(errors::QueryAddSnafu)?)
    }

    async fn get_query(&self, id: QueryRecordId) -> HistoryStoreResult<QueryRecord> {
        let key_bytes = QueryRecord::get_key(id);
        let key_str = std::str::from_utf8(key_bytes.as_ref()).context(errors::BadKeySnafu)?;

        let res: Option<QueryRecord> = self.db.get(key_str).await.context(errors::QueryGetSnafu)?;
        res.ok_or_else(|| HistoryStoreError::QueryNotFound {
            key: key_str.to_string(),
        })
    }

    async fn get_queries(&self, params: GetQueriesParams) -> HistoryStoreResult<Vec<QueryRecord>> {
        let GetQueriesParams {
            worksheet_id,
            sql_text: _,
            min_duration_ms: _,
            cursor,
            limit,
        } = params;

        if let Some(worksheet_id) = worksheet_id {
            // 1. Get iterator over all queries references related to a worksheet_id (QueryRecordReference)
            let mut refs_iter =
                worksheet_queries_references_iterator(&self.db, worksheet_id, cursor).await?;

            // 2. Get iterator over all queries (QueryRecord)
            let mut queries_iter = queries_iterator(&self.db, cursor).await?;

            // 3. Loop over query record references, get record keys by their references
            // 4. Extract records by their keys

            let mut items: Vec<QueryRecord> = vec![];
            while let Ok(Some(item)) = refs_iter.next().await {
                let qh_key = QueryRecordReference::extract_qh_key(&item.key).ok_or(
                    HistoryStoreError::QueryReferenceKey {
                        key: format!("{:?}", item.key),
                    },
                )?;
                queries_iter.seek(qh_key).await.context(errors::SeekSnafu)?;
                match queries_iter.next().await {
                    Ok(Some(query_record_kv)) => {
                        items.push(
                            de::from_slice(&query_record_kv.value)
                                .context(errors::DeserializeValueSnafu)?,
                        );
                        if items.len() >= usize::from(limit.unwrap_or(u16::MAX)) {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            Ok(items)
        } else {
            let start_key = QueryRecord::get_key(cursor.unwrap_or_else(QueryRecordId::min_cursor));
            let end_key = QueryRecord::get_key(QueryRecordId::max_cursor());

            Ok(self
                .db
                .items_from_range(start_key..end_key, limit)
                .await
                .context(errors::QueryGetSnafu)?)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::entities::query::{
        ExecutionQueryRecord, MockExecutionQueryRecord, QueryRecord, QueryStatus,
    };
    use crate::entities::worksheet::Worksheet;
    use chrono::{Duration, TimeZone, Utc};
    use core_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    fn create_query_records(templates: &[(Option<i64>, QueryStatus)]) -> Vec<QueryRecord> {
        let mut created: Vec<QueryRecord> = vec![];
        for (i, (worksheet_id, query_status)) in templates.iter().enumerate() {
            let ctx = MockExecutionQueryRecord::query_start_context();
            ctx.expect().returning(move |query, worksheet_id| {
                let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                    + Duration::milliseconds(
                        i.try_into().expect("Failed convert idx to milliseconds"),
                    );
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
                QueryStatus::Running => MockExecutionQueryRecord::query_start(
                    format!("select {i}").as_str(),
                    *worksheet_id,
                ),
                QueryStatus::Successful => {
                    let mut item = MockExecutionQueryRecord::query_start(
                        format!("select {i}").as_str(),
                        *worksheet_id,
                    );
                    item.query_finished(1, Some(String::from("pseudo result")));
                    item
                }
                QueryStatus::Failed => {
                    let mut item = MockExecutionQueryRecord::query_start(
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
        let db = SlateDBHistoryStore::new_in_memory().await;

        // create a worksheet first
        let worksheet = Worksheet::new(String::new(), String::new());
        let worksheet = db
            .add_worksheet(worksheet)
            .await
            .expect("Failed creating worksheet");

        let created = create_query_records(&[
            (Some(worksheet.id), QueryStatus::Successful),
            (Some(worksheet.id), QueryStatus::Failed),
            (Some(worksheet.id), QueryStatus::Running),
            (None, QueryStatus::Running),
        ]);

        for item in &created {
            eprintln!("added {:?}", item.key());
            db.add_query(item).await.expect("Failed adding query");
        }

        let cursor = <QueryRecord as IterableEntity>::Cursor::min_cursor();
        eprintln!("cursor: {cursor}");
        let get_queries_params = GetQueriesParams::new()
            .with_worksheet_id(worksheet.id)
            .with_cursor(cursor)
            .with_limit(10);
        let retrieved = db
            .get_queries(get_queries_params)
            .await
            .expect("Failed getting queries");
        // queries belong to the worksheet
        assert_eq!(3, retrieved.len());

        let get_queries_params = GetQueriesParams::new().with_cursor(cursor).with_limit(10);
        let retrieved_all = db
            .get_queries(get_queries_params)
            .await
            .expect("Failed getting queries");
        // all queries
        for item in &retrieved_all {
            eprintln!("retrieved_all : {:?}", item.key());
        }
        assert_eq!(created.len(), retrieved_all.len());
        assert_eq!(created, retrieved_all);

        // Delete worksheet & check related keys
        db.delete_worksheet(worksheet.id)
            .await
            .expect("Failed deleting worksheet");
        let mut worksheet_refs_iter =
            worksheet_queries_references_iterator(&db.db, worksheet.id, None)
                .await
                .expect("Error getting worksheets queries references iterator");
        let mut rudiment_keys = vec![];
        while let Ok(Some(item)) = worksheet_refs_iter.next().await {
            eprintln!("rudiment key left after worksheet deleted: {:?}", item.key);
            rudiment_keys.push(item.key);
        }
        assert_eq!(rudiment_keys.len(), 0);
    }
}
