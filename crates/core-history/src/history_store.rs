use crate::errors::{self as core_history_errors, Result};
use crate::result_set::ResultSet;
use crate::{
    QueryRecord, QueryRecordId, QueryRecordReference, QueryStatus, SlateDBHistoryStore, Worksheet,
    WorksheetId,
};
use async_trait::async_trait;
use core_utils::Db;
use core_utils::iterable::IterableCursor;
use futures::future::join_all;
use serde_json::de;
use slatedb::DbIterator;
use snafu::ResultExt;
use tracing::instrument;

#[derive(Default, Clone, Debug)]
pub enum SortOrder {
    Ascending,
    #[default]
    Descending,
}

#[derive(Debug)]
pub struct QueryResultError {
    // additional error status like: cancelled, timeout, etc
    pub status: QueryStatus,
    pub message: String,
    pub diagnostic_message: String,
}
impl std::fmt::Display for QueryResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // do not output status, it is just an internal context
        write!(
            f,
            "QueryResultError: {} | Diagnostic: {}",
            self.message, self.diagnostic_message
        )
    }
}

impl std::error::Error for QueryResultError {}

#[derive(Default, Debug)]
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

#[mockall::automock]
#[async_trait]
pub trait HistoryStore: std::fmt::Debug + Send + Sync {
    async fn add_worksheet(&self, worksheet: Worksheet) -> Result<Worksheet>;
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet>;
    async fn update_worksheet(&self, worksheet: Worksheet) -> Result<()>;
    async fn delete_worksheet(&self, id: WorksheetId) -> Result<()>;
    async fn get_worksheets(&self) -> Result<Vec<Worksheet>>;
    async fn add_query(&self, item: &QueryRecord) -> Result<()>;
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord>;
    async fn get_queries(&self, params: GetQueriesParams) -> Result<Vec<QueryRecord>>;
    fn query_record(&self, query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord;
    async fn save_query_record(
        &self,
        query_record: &mut QueryRecord,
        execution_result: Option<std::result::Result<ResultSet, QueryResultError>>,
    );
}

async fn queries_iterator(db: &Db, cursor: Option<QueryRecordId>) -> Result<DbIterator<'_>> {
    let start_key = QueryRecord::get_key(cursor.map_or_else(i64::min_cursor, Into::into));
    let end_key = QueryRecord::get_key(i64::max_cursor());
    db.range_iterator(start_key..end_key)
        .await
        .context(core_history_errors::GetWorksheetQueriesSnafu)
}

async fn worksheet_queries_references_iterator(
    db: &Db,
    worksheet_id: WorksheetId,
    cursor: Option<QueryRecordId>,
) -> Result<DbIterator<'_>> {
    let refs_start_key = QueryRecordReference::get_key(
        worksheet_id,
        cursor.unwrap_or_else(|| i64::min_cursor().into()),
    );
    let refs_end_key = QueryRecordReference::get_key(worksheet_id, i64::max_cursor().into());
    db.range_iterator(refs_start_key..refs_end_key)
        .await
        .context(core_history_errors::GetWorksheetQueriesSnafu)
}

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    #[instrument(
        name = "HistoryStore::add_worksheet",
        level = "debug",
        skip(self, worksheet),
        err
    )]
    async fn add_worksheet(&self, worksheet: Worksheet) -> Result<Worksheet> {
        self.db
            .put_iterable_entity(&worksheet)
            .await
            .context(core_history_errors::WorksheetAddSnafu)?;
        Ok(worksheet)
    }

    #[instrument(name = "HistoryStore::get_worksheet", level = "debug", skip(self), err)]
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet> {
        // convert from Bytes to &str, for .get method to convert it back to Bytes
        let key_bytes = Worksheet::get_key(id);
        let key_str =
            std::str::from_utf8(key_bytes.as_ref()).context(core_history_errors::BadKeySnafu)?;

        let res: Option<Worksheet> = self
            .db
            .get(key_str)
            .await
            .context(core_history_errors::WorksheetGetSnafu)?;
        res.ok_or_else(|| {
            core_history_errors::WorksheetNotFoundSnafu {
                message: key_str.to_string(),
            }
            .build()
        })
    }

    #[instrument(name = "HistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None);

        Ok(self
            .db
            .put_iterable_entity(&worksheet)
            .await
            .context(core_history_errors::WorksheetUpdateSnafu)?)
    }

    #[instrument(
        name = "HistoryStore::delete_worksheet",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_worksheet(&self, id: WorksheetId) -> Result<()> {
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
            .context(core_history_errors::WorksheetDeleteSnafu)?)
    }

    #[instrument(
        name = "HistoryStore::get_worksheets",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_worksheets(&self) -> Result<Vec<Worksheet>> {
        let start_key = Worksheet::get_key(WorksheetId::min_cursor());
        let end_key = Worksheet::get_key(WorksheetId::max_cursor());
        Ok(self
            .db
            .items_from_range(start_key..end_key, None)
            .await
            .context(core_history_errors::WorksheetsListSnafu)?)
    }

    #[instrument(
        name = "HistoryStore::add_query",
        level = "debug",
        skip(self, item),
        err
    )]
    async fn add_query(&self, item: &QueryRecord) -> Result<()> {
        if let Some(worksheet_id) = item.worksheet_id {
            // add query reference to the worksheet
            self.db
                .put_iterable_entity(&QueryRecordReference {
                    id: item.id,
                    worksheet_id,
                })
                .await
                .context(core_history_errors::QueryReferenceAddSnafu)?;
        }

        // add query record
        Ok(self
            .db
            .put_iterable_entity(item)
            .await
            .context(core_history_errors::QueryAddSnafu)?)
    }

    #[instrument(name = "HistoryStore::get_query", level = "debug", skip(self), err)]
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord> {
        let key_bytes = QueryRecord::get_key(id.into());
        let key_str =
            std::str::from_utf8(key_bytes.as_ref()).context(core_history_errors::BadKeySnafu)?;

        let res: Option<QueryRecord> = self
            .db
            .get(key_str)
            .await
            .context(core_history_errors::QueryGetSnafu)?;
        res.ok_or_else(|| core_history_errors::QueryNotFoundSnafu { query_id: id }.build())
    }

    #[instrument(name = "HistoryStore::get_queries", level = "debug", skip(self), err)]
    async fn get_queries(&self, params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
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
                let qh_key = QueryRecordReference::extract_qh_key(&item.key).ok_or_else(|| {
                    core_history_errors::QueryReferenceKeySnafu {
                        key: format!("{:?}", item.key),
                    }
                    .build()
                })?;
                queries_iter
                    .seek(qh_key)
                    .await
                    .context(core_history_errors::SeekSnafu)?;
                match queries_iter.next().await {
                    Ok(Some(query_record_kv)) => {
                        items.push(
                            de::from_slice(&query_record_kv.value)
                                .context(core_history_errors::DeserializeValueSnafu)?,
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
            let start_key = QueryRecord::get_key(cursor.map_or_else(i64::min_cursor, Into::into));
            let end_key = QueryRecord::get_key(i64::max_cursor());

            Ok(self
                .db
                .items_from_range(start_key..end_key, limit)
                .await
                .context(core_history_errors::QueryGetSnafu)?)
        }
    }

    fn query_record(&self, query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord {
        QueryRecord::new(query, worksheet_id)
    }

    #[instrument(
        name = "SlateDBHistoryStore::save_query_record",
        level = "trace",
        skip(self, query_record, execution_result),
        fields(query_id = query_record.id.as_i64(),
            query = query_record.query,
            query_result_count = query_record.result_count,
            query_duration_ms = query_record.duration_ms,
            query_status = format!("{:?}", query_record.status),
            error = query_record.error,
            save_query_history_error,
        ),
    )]
    async fn save_query_record(
        &self,
        query_record: &mut QueryRecord,
        execution_result: Option<std::result::Result<ResultSet, QueryResultError>>,
    ) {
        if let Some(execution_result) = execution_result {
            match execution_result {
                Ok(result_set) => match serde_json::to_string(&result_set) {
                    Ok(encoded_res) => {
                        let result_count = i64::try_from(result_set.rows.len()).unwrap_or(0);
                        query_record.finished(result_count, Some(encoded_res));
                    }
                    // serde error
                    Err(err) => query_record.finished_with_error(QueryResultError {
                        status: QueryStatus::Failed,
                        message: err.to_string(),
                        diagnostic_message: format!("{err:?}"),
                    }),
                },
                Err(execution_err) => query_record.finished_with_error(execution_err),
            }
        }

        if let Err(err) = self.add_query(query_record).await {
            // Record the result as part of the current span.
            tracing::Span::current().record("save_query_history_error", format!("{err:?}"));

            tracing::error!("Failed to record query history: {err}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::entities::query::{QueryRecord, QueryStatus};
    use crate::entities::worksheet::Worksheet;
    use chrono::{Duration, TimeZone, Utc};
    use core_utils::iterable::{IterableCursor, IterableEntity};
    use tokio;

    fn create_query_records(templates: &[(Option<i64>, QueryStatus)]) -> Vec<QueryRecord> {
        let mut created: Vec<QueryRecord> = vec![];
        for (i, (worksheet_id, query_status)) in templates.iter().enumerate() {
            let query_record_fn = |query: &str, worksheet_id: Option<WorksheetId>| -> QueryRecord {
                let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                    + Duration::milliseconds(
                        i.try_into().expect("Failed convert idx to milliseconds"),
                    );
                let mut record = QueryRecord::new(query, worksheet_id);
                record.id = QueryRecordId(start_time.timestamp_millis());
                record.start_time = start_time;
                record.status = QueryStatus::Running;
                record
            };
            let query_record = match query_status {
                QueryStatus::Running => {
                    query_record_fn(format!("select {i}").as_str(), *worksheet_id)
                }
                QueryStatus::Successful => {
                    let mut item = query_record_fn(format!("select {i}").as_str(), *worksheet_id);
                    item.finished(1, Some(String::from("pseudo result")));
                    item
                }
                QueryStatus::Canceled | QueryStatus::TimedOut | QueryStatus::Failed => {
                    let mut item = query_record_fn(format!("select {i}").as_str(), *worksheet_id);
                    item.finished_with_error(QueryResultError {
                        status: query_status.clone(),
                        message: String::from("Test query pseudo error"),
                        diagnostic_message: String::from("diagnostic message"),
                    });
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

        let cursor = QueryRecordId(<QueryRecord as IterableEntity>::Cursor::min_cursor());
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
