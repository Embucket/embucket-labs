use crate::errors::{self as core_history_errors, Result};
use crate::interface::{GetQueriesParams, HistoryStore};
use crate::{
    QueryRecord, QueryRecordId, QueryRecordReference, SlateDBHistoryStore, Worksheet, WorksheetId,
};
use async_trait::async_trait;
use core_utils::Db;
use core_utils::iterable::IterableCursor;
use futures::future::join_all;
use serde_json::de;
use slatedb::DbIterator;
use snafu::OptionExt;
use snafu::ResultExt;
use tracing::instrument;

pub struct SlateDBHistoryStore {
    pub db: Db,
}

impl std::fmt::Debug for SlateDBHistoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBHistoryStore").finish()
    }
}

impl SlateDBHistoryStore {
    #[allow(clippy::expect_used)]
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Self {
        // create utils db regardless of feature, but use it only with utilsdb feature
        // to avoid changing the code
        let utils_db = Db::memory().await;
        Self::new(utils_db)
    }

    #[must_use]
    pub const fn db(&self) -> &Db {
        &self.db
    }
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
        Ok(res.context(core_history_errors::QueryNotFoundSnafu { query_id: id })?)
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
        skip(self, query_record),
        fields(query_id = query_record.id.as_i64(),
            query = query_record.query,
            query_result_count = query_record.result_count,
            query_duration_ms = query_record.duration_ms,
            query_status = format!("{:?}", query_record.status),
            error = query_record.error,
            save_query_history_error,
        ),
    )]
    async fn save_query_record(&self, query_record: &mut QueryRecord) {
        // This function won't fail, just sends happened write errors to the logs
        if let Err(err) = self.add_query(query_record).await {
            // Record the result as part of the current span.
            tracing::Span::current().record("save_query_history_error", format!("{err:?}"));

            tracing::error!(error = %err, "Failed to record query history");
        }
    }
}
