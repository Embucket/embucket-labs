use crate::errors::{self as history_err, Result};
use crate::interface::{HistoryStore, GetQueriesParams};
use crate::{QueryStatus, QueryRecord, QueryRecordId, QueryRecordReference, SlateDBHistoryStore, Worksheet, WorksheetId};
use core_sqlite::error as core_sqlite_err;
use core_utils::errors::{self as core_utils_err};
use async_trait::async_trait;
use core_utils::iterable::IterableCursor;
use rusqlite::Result as SqlResult;
use rusqlite::named_params;
use snafu::OptionExt;
use snafu::ResultExt;
use tracing::instrument;
use uuid::Uuid;

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
    result TEXT,                        -- FK -> results.id
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
    #[instrument(
        name = "SqliteHistoryStore::create_tables",
        level = "debug",
        skip(self),
        err
    )]
    pub async fn create_tables(&self) -> Result<()> {
        let connection = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::CoreUtilsSnafu)?;

        let _res = connection.interact(move |conn| -> SqlResult<usize> {
            conn.execute(QUERIES_CREATE_TABLE, [])?;
            conn.execute(WORKSHEETS_CREATE_TABLE, [])?;
            conn.execute(RESULTS_CREATE_TABLE, [])
        })
        .await?
        .context(history_err::CreateTablesSnafu)?;
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
        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let sql = WORKSHEET_ADD.to_string();
        let worksheet_cloned = worksheet.clone();
        let _res = conn.interact(move |conn| -> SqlResult<usize> {
                let params = named_params! {
                    ":id": worksheet_cloned.id,
                    ":name": worksheet_cloned.name,
                    ":content": worksheet_cloned.content,
                    ":created_at": worksheet_cloned.created_at.to_rfc3339(),
                    ":updated_at": worksheet_cloned.updated_at.to_rfc3339(),
                };
                conn.execute(&sql, params)
            })
            .await?
            .context(core_utils_err::RuSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;
        Ok(worksheet)
    }

    #[instrument(name = "SqliteHistoryStore::get_worksheet", level = "debug", skip(self), err)]
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet> {
        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetGetSnafu)?;

        let res = conn.interact(move |conn| -> SqlResult<Worksheet> {
            let mut stmt = conn.prepare(
                "SELECT id, name, content, created_at, updated_at FROM worksheets WHERE id = ?1",
            )?;

            stmt.query_row([id], |row| {
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
            })
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::WorksheetGetSnafu)?;
        Ok(res)
    }

    #[instrument(name = "SqliteHistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None); // set current time

        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetUpdateSnafu)?;

        let _res = conn.interact(move |conn| -> SqlResult<usize> {
            conn.execute(
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
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::WorksheetUpdateSnafu)?;

        Ok(())
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
        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetGetSnafu)?;

        let res = conn.interact(|conn| -> SqlResult<Vec<Worksheet>> {
            let mut stmt = conn.prepare(
                "SELECT id, name, content, created_at, updated_at FROM worksheets ORDER BY created_at DESC",
            )?;
    
            let rows = stmt.query_map([], |row| {
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
            })?;
    
            let mut results = Vec::new();
            for ws in rows {
                results.push(ws?);
            }
    
            Ok(results)
        }).await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::WorksheetsListSnafu)?;

        Ok(res)
    }

    #[instrument(
        name = "SqliteHistoryStore::add_query",
        level = "debug",
        skip(self),
        err
    )]
    async fn add_query(&self, item: &QueryRecord) -> Result<()> {
        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let q = item.clone();
        conn.interact(move |conn| -> SqlResult<usize> {
            conn.execute(
                "INSERT INTO queries (
                    id, worksheet_id, query, start_time, end_time,
                    duration_ms, result_count, result, status, error, diagnostic_error
                    )
                    VALUES (
                    :id, :worksheet_id, :query, :start_time, :end_time,
                    :duration_ms, :result_count, :result, :status, :error, :diagnostic_error
                    )",
                named_params! {
                    ":id": q.id.to_string(),
                    ":worksheet_id": q.worksheet_id.as_ref().map(|id| id.to_string()),
                    ":query": q.query,
                    ":start_time": q.start_time.to_rfc3339(),
                    ":end_time": q.end_time.to_rfc3339(),
                    ":duration_ms": q.duration_ms,
                    ":result_count": q.result_count,
                    ":result": q.result,
                    ":status": q.status.to_string(),
                    ":error": q.error,
                    ":diagnostic_error": q.diagnostic_error,
                },
            )
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::QueryAddSnafu)?;
        Ok(())
    }

    #[instrument(name = "SqliteHistoryStore::get_query", level = "debug", skip(self), err)]
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord> {
        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let res = conn.interact(move |conn| -> SqlResult<QueryRecord> {
            let mut stmt = conn.prepare(
                "SELECT id, name, content, created_at, updated_at FROM worksheets WHERE id = ?1",
            )?;

            stmt.query_row([id.to_string()], |row| {
                Ok(QueryRecord {
                    id: row.get::<_, String>(0)?.as_str().parse::<QueryRecordId>().unwrap(),
                    worksheet_id: row.get::<_, Option<String>>(1)?.map(|s| s.parse::<i64>().unwrap()),
                    query: row.get(2)?,
                    start_time: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                    end_time: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                    duration_ms: row.get(5)?,
                    result_count: row.get(6)?,
                    result: row.get(7)?,
                    status: row.get::<_, String>(8)?.as_str().parse::<QueryStatus>().unwrap(),
                    error: row.get(9)?,
                    diagnostic_error: row.get(10)?,
                })
            })
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::QueryGetSnafu)?;
        Ok(res)
        //     .context(history_err::QueryGetSnafu)?;
        // Ok(res.context(history_err::QueryNotFoundSnafu { query_id: id })?)
    }

    #[instrument(name = "SqliteHistoryStore::get_queries", level = "debug", skip(self), err)]
    async fn get_queries(&self, _params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
        let items: Vec<QueryRecord> = vec![];

        let conn = self.db.sqlite.conn(DB_NAME).await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let items = conn.interact(|conn| -> SqlResult<Vec<QueryRecord>> {
            let mut stmt = conn.prepare(
                "SELECT id, worksheet_id, query, start_time, end_time,
                        duration_ms, result_count, result, status, error, diagnostic_error
                 FROM queries",
            )?;
    
            let rows = stmt.query_map([], |row| {
                Ok(QueryRecord {
                    id: row.get::<_, String>(0)?.as_str().parse::<QueryRecordId>().unwrap(),
                    worksheet_id: row.get::<_, Option<String>>(1)?.map(|s| s.parse::<i64>().unwrap()),
                    query: row.get(2)?,
                    start_time: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                    end_time: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                    duration_ms: row.get(5)?,
                    result_count: row.get(6)?,
                    result: row.get(7)?,
                    status: row.get::<_, String>(8)?.as_str().parse::<QueryStatus>().unwrap(),
                    error: row.get(9)?,
                    diagnostic_error: row.get(10)?,
                })
            })?;

            let mut results = Vec::new();
            for ws in rows {
                results.push(ws?);
            }

            Ok(results)
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::QueryGetSnafu)?;
        
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