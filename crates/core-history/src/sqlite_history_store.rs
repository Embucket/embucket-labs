use crate::errors::{self as history_err, Result};
use crate::interface::{HistoryStore, GetQueriesParams};
use crate::{QueryRecord, QueryRecordId, QueryRecordReference, SlateDBHistoryStore, Worksheet, WorksheetId};
use core_sqlite::error as core_sqlite_err;
use core_utils::errors::{self as core_utils_err};
use async_trait::async_trait;
use core_utils::iterable::IterableCursor;
use rusqlite::Result as SqlResult;
use rusqlite::named_params;
use snafu::OptionExt;
use snafu::ResultExt;
use tracing::instrument;

pub const DB_NAME: &str = "query_history.db";


// let result = connection.interact(|conn| -> SqlResult<Vec<String>> {
//     let mut stmt = conn.prepare("SELECT name FROM sqlite_schema WHERE type ='table'")?;
//     let mut rows = stmt.query([])?;
//     let mut out = Vec::new();
//     while let Some(row) = rows.next()? {
//         out.push(row.get(0)?);
//     }
//     Ok(out)
// })
// .await
// .context(sqlite_error::InteractSnafu)?
// .context(sqlite_error::RusqliteSnafu)?;

const RESULTS_CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,                -- using TEXT for timestamp (ISO8601)
    result TEXT                         -- JSON or text result payload
);"#;

const WORKSHEETS_CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS worksheets (
    id TEXT PRIMARY KEY,                -- UUID as TEXT
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL,           -- stored as ISO8601 timestamp
    updated_at TEXT NOT NULL
);"#;

const QUERIES_CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,                -- UUID
    worksheet_id TEXT,                  -- FK -> worksheets.id
    query TEXT NOT NULL,
    start_time TEXT NOT NULL,           -- ISO8601 UTC
    end_time TEXT NOT NULL,             -- ISO8601 UTC
    duration_ms INTEGER NOT NULL,
    result_count INTEGER NOT NULL,
    result TEXT NOT NULL,               -- FK -> results.id
    status TEXT NOT NULL,               -- enum as TEXT
    error TEXT,                         -- nullable
    diagnostic_error TEXT,              -- nullable
    FOREIGN KEY (worksheet_id) REFERENCES worksheets (id) ON DELETE SET NULL,
    FOREIGN KEY (result) REFERENCES results (id) ON DELETE CASCADE
);"#;

const WORKSHEET_ADD: &str = r#"
INSERT INTO worksheets (id, name, content, created_at, updated_at)
    VALUES (:id, :name, :content, :created_at, :updated_at);
"#;

impl SlateDBHistoryStore {

    // with life time params
    pub async fn execute(&self, sql: &str) -> Result<usize> {
        let connection = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::CoreUtilsSnafu)?;

        let sql = sql.to_string();
        let res = connection
            .interact(move |conn| {
                conn
                .execute(&sql, [])
            })
            .await
            .context(core_utils_err::ConnectorSnafu)
            .context(history_err::CoreUtilsSnafu)?
            .context(history_err::SqliteSnafu)?;
        Ok(res)
    }

    #[instrument(
        name = "SqliteHistoryStore::create_tables",
        level = "debug",
        skip(self),
        err
    )]
    pub async fn create_tables(&self) -> Result<()> {
        self.execute(QUERIES_CREATE_TABLE).await?;
        self.execute(WORKSHEETS_CREATE_TABLE).await?;
        self.execute(RESULTS_CREATE_TABLE).await?;
        Ok(())
    }
}

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    #[instrument(
        name = "SqliteHistoryStore::add_worksheet",
        level = "debug",
        skip(self, worksheet),
        err
    )]
    async fn add_worksheet(&self, worksheet: Worksheet) -> Result<Worksheet> {
        let connection = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let sql = WORKSHEET_ADD.to_string();
        let worksheet_cloned = worksheet.clone();
        let _res = connection
            .interact(move |conn| -> Result<usize> {
                let params = named_params! {
                    ":id": worksheet_cloned.id,
                    ":name": worksheet_cloned.name,
                    ":content": worksheet_cloned.content,
                    ":created_at": worksheet_cloned.created_at.to_rfc3339(),
                    ":updated_at": worksheet_cloned.updated_at.to_rfc3339(),
                };
                let res = conn.execute(&sql, params)
                    .context(core_sqlite_err::RusqliteSnafu)
                    .context(core_utils_err::CoreSqliteSnafu)
                    .context(history_err::WorksheetAddSnafu)?;
                Ok(res)
            })
            .await
            .context(core_utils_err::ConnectorSnafu)
            .context(history_err::WorksheetAddSnafu)??;
        Ok(worksheet)
    }

    #[instrument(name = "SqliteHistoryStore::get_worksheet", level = "debug", skip(self), err)]
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet> {
        let connection = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetGetSnafu)?;

        let res = connection.interact(move |conn| -> Result<Worksheet> {
            let mut stmt = conn.prepare(
                "SELECT id, name, content, created_at, updated_at FROM worksheets WHERE id = ?1",
            )
            .context(core_sqlite_err::RusqliteSnafu)
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetGetSnafu)?;

            let result = stmt.query_row([id], |row| {
                Ok(Worksheet {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    content: row.get(2)?,
                    created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                    updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                })
            });

            match result {
                Ok(ws) => Ok(ws),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    history_err::WorksheetNotFoundSnafu {
                        message: id.to_string()
                    }.fail()
                },
                Err(e) => {
                    Err(e)
                        .context(core_sqlite_err::RusqliteSnafu)
                        .context(core_utils_err::CoreSqliteSnafu)
                        .context(history_err::WorksheetGetSnafu)
                }
            }
        })
        .await
        .context(core_utils_err::ConnectorSnafu)
        .context(history_err::WorksheetGetSnafu)?;
        res
    }

    #[instrument(name = "SqliteHistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None); // set current time

        let connection = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetUpdateSnafu)?;

        let res = connection.interact(move |conn| {
            let res = conn.execute(
                "UPDATE worksheets
                 SET name = :name, content = :content, updated_at = :updated_at
                 WHERE id = :id",
                named_params! {
                    ":id": worksheet.id,
                    ":name": worksheet.name,
                    ":content": worksheet.content,
                    ":updated_at": worksheet.updated_at.to_rfc3339(),
                },
            )
            .context(core_sqlite_err::RusqliteSnafu)
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetUpdateSnafu)?;
            Ok(())
        })
        .await
        .context(core_utils_err::ConnectorSnafu)
        .context(history_err::WorksheetUpdateSnafu)?;

        res
    }

    #[instrument(
        name = "SqliteHistoryStore::delete_worksheet",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_worksheet(&self, _id: WorksheetId) -> Result<()> {
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
            .context(history_err::WorksheetsListSnafu)?)
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
                .context(history_err::QueryReferenceAddSnafu)?;
        }

        // add query record
        Ok(self
            .db
            .put_iterable_entity(item)
            .await
            .context(history_err::QueryAddSnafu)?)
    }

    #[instrument(name = "SqliteHistoryStore::get_query", level = "debug", skip(self), err)]
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord> {
        let key_bytes = QueryRecord::get_key(id.into());
        let key_str =
            std::str::from_utf8(key_bytes.as_ref()).context(history_err::BadKeySnafu)?;

        let res: Option<QueryRecord> = self
            .db
            .get(key_str)
            .await
            .context(history_err::QueryGetSnafu)?;
        Ok(res.context(history_err::QueryNotFoundSnafu { query_id: id })?)
    }

    #[instrument(name = "SqliteHistoryStore::get_queries", level = "debug", skip(self), err)]
    async fn get_queries(&self, _params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
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
            save_query_history_errror,
        ),
    )]
    async fn save_query_record(&self, query_record: &mut QueryRecord) {
        // This function won't fail, just sends happened write errors to the logs
        if let Err(err) = self.add_query(query_record).await {
            // Record the result as part of the current span.
            tracing::Span::current().record("save_query_history_errror", format!("{err:?}"));

            tracing::error!(error = %err, "Failed to record query history");
        }
    }
}