use crate::errors::{self as core_history_errors, Result};
use crate::interface::{HistoryStore, GetQueriesParams};
use crate::{QueryRecord, QueryRecordId, QueryRecordReference, SlateDBHistoryStore, Worksheet, WorksheetId};
use async_trait::async_trait;
use core_utils::iterable::IterableCursor;
use futures::future::join_all;
use serde_json::de;
use snafu::OptionExt;
use snafu::ResultExt;
use tracing::instrument;

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    #[instrument(
        name = "SqliteHistoryStore::add_worksheet",
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

    #[instrument(name = "SqliteHistoryStore::get_worksheet", level = "debug", skip(self), err)]
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

    #[instrument(name = "SqliteHistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None);

        Ok(self
            .db
            .put_iterable_entity(&worksheet)
            .await
            .context(core_history_errors::WorksheetUpdateSnafu)?)
    }

    #[instrument(
        name = "SqliteHistoryStore::delete_worksheet",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_worksheet(&self, id: WorksheetId) -> Result<()> {
        Ok(())
    }

    #[instrument(
        name = "SqliteHistoryStore::get_worksheets",
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
        name = "SqliteHistoryStore::add_query",
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

    #[instrument(name = "SqliteHistoryStore::get_query", level = "debug", skip(self), err)]
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

    #[instrument(name = "SqliteHistoryStore::get_queries", level = "debug", skip(self), err)]
    async fn get_queries(&self, params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
        let items: Vec<QueryRecord> = vec![];
        Ok(items)
    }

    fn query_record(&self, query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord {
        QueryRecord::new(query, worksheet_id)
    }

    #[instrument(
        name = "SlateDBSqliteHistoryStore::save_query_record",
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