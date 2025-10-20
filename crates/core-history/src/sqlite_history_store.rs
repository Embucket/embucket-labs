use crate::errors::{self as history_err, Result};
use crate::interface::{GetQueriesParams, HistoryStore};
use crate::{QueryRecord, QueryRecordId, QueryStatus, SlateDBHistoryStore, Worksheet, WorksheetId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_utils::errors::{self as core_utils_err};
use rusqlite::Result as SqlResult;
use rusqlite::named_params;
use snafu::ResultExt;
use tracing::instrument;

const RESULTS_CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,                -- using TEXT for timestamp (ISO8601)
    result TEXT NOT NULL                -- JSON or text result payload
);";

const WORKSHEETS_CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS worksheets (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL,           -- stored as ISO8601 timestamp
    updated_at TEXT NOT NULL
);";

const QUERIES_CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,                -- UUID
    worksheet_id INTEGER,               -- FK -> worksheets.id
    result_id TEXT,                     -- FK -> results.id
    query TEXT NOT NULL,
    start_time TEXT NOT NULL,           -- ISO8601 UTC
    end_time TEXT NOT NULL,             -- ISO8601 UTC
    duration_ms INTEGER NOT NULL,
    result_count INTEGER NOT NULL,
    status TEXT NOT NULL,               -- enum as TEXT
    error TEXT,                         -- nullable
    diagnostic_error TEXT,              -- nullable
    FOREIGN KEY (worksheet_id) REFERENCES worksheets (id) ON DELETE SET NULL
);";

const WORKSHEET_ADD: &str = "
INSERT INTO worksheets (id, name, content, created_at, updated_at)
    VALUES (:id, :name, :content, :created_at, :updated_at);
";

impl SlateDBHistoryStore {
    #[instrument(
        name = "SqliteHistoryStore::create_tables",
        level = "debug",
        skip(self),
        err
    )]
    pub async fn create_tables(&self) -> Result<()> {
        let connection = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::CoreUtilsSnafu)?;

        let _res = connection
            .interact(move |conn| -> SqlResult<usize> {
                conn.execute("BEGIN", [])?;
                conn.execute(WORKSHEETS_CREATE_TABLE, [])?;
                conn.execute(RESULTS_CREATE_TABLE, [])?;
                conn.execute(QUERIES_CREATE_TABLE, [])?;
                conn.execute("COMMIT", [])
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
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let sql = WORKSHEET_ADD.to_string();
        let worksheet_cloned = worksheet.clone();
        let _res = conn
            .interact(move |conn| -> SqlResult<usize> {
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

    #[instrument(
        name = "SqliteHistoryStore::get_worksheet",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet> {
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetGetSnafu)?;

        let res = conn
            .interact(move |conn| -> SqlResult<Worksheet> {
                let mut stmt = conn.prepare(
                "SELECT id, name, content, created_at, updated_at FROM worksheets WHERE id = ?1",
            )?;

                stmt.query_row([id], |row| {
                    Ok(Worksheet {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        content: row.get(2)?,
                        created_at: parse_date(&row.get::<_, String>(3)?)?,
                        updated_at: parse_date(&row.get::<_, String>(4)?)?,
                    })
                })
            })
            .await?;

        if res == Err(rusqlite::Error::QueryReturnedNoRows) {
            history_err::WorksheetNotFoundSnafu {
                message: id.to_string(),
            }
            .fail()
        } else {
            res.context(core_utils_err::RuSqliteSnafu)
                .context(history_err::WorksheetGetSnafu)
        }
    }

    #[instrument(name = "SqliteHistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None); // set current time

        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetUpdateSnafu)?;

        let _res = conn
            .interact(move |conn| -> SqlResult<usize> {
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
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
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
                    created_at: parse_date(&row.get::<_, String>(3)?)?,
                    updated_at: parse_date(&row.get::<_, String>(4)?)?,
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
        skip(self, item),
        fields(item = format!("{item:#?}")),
        err
    )]
    async fn add_query(&self, item: &QueryRecord) -> Result<()> {
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let q = item.clone();
        conn.interact(move |conn| -> SqlResult<usize> {
            conn.execute(
                "INSERT INTO queries (
                    id, worksheet_id, query, start_time, end_time,
                    duration_ms, result_count, result_id, status, error, diagnostic_error
                    )
                    VALUES (
                    :id, :worksheet_id, :query, :start_time, :end_time,
                    :duration_ms, :result_count, :result_id, :status, :error, :diagnostic_error
                    )",
                named_params! {
                    ":id": q.id.to_string(),
                    ":worksheet_id": q.worksheet_id,
                    ":query": q.query,
                    ":start_time": q.start_time.to_rfc3339(),
                    ":end_time": q.end_time.to_rfc3339(),
                    ":duration_ms": q.duration_ms,
                    ":result_count": q.result_count,
                    ":result_id": None::<String>,
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

    #[instrument(
        name = "SqliteHistoryStore::update_query",
        level = "debug",
        skip(self, item),
        err
    )]
    async fn update_query(&self, item: &QueryRecord) -> Result<()> {
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let q = item.clone();

        let q_uuid = q.id.as_uuid();
        let q_id = q.id.to_string();
        let q_status = q.status.to_string();
        let q_end_time = q.end_time.to_rfc3339();
        let q_duration_ms = q.duration_ms;
        let q_result_count = q.result_count;
        let q_result = q.result;
        let q_error = q.error;
        let q_diagnostic_error = q.diagnostic_error;

        tracing::info!("update_query: {q_id} / {q_uuid} status={q_status}");

        conn.interact(move |conn| -> SqlResult<usize> {
            // at firt insert result, to satisfy contrint in update stmt
            if let Some(result) = q_result {
                tracing::info!(
                    "INSERT INTO results ({q_id} / {q_uuid}) result_len={}",
                    result.len()
                );
                conn.execute(
                    "INSERT INTO results (id, result)
                        VALUES (:id, :result)",
                    named_params! {
                        ":id": q_id,
                        ":result": result,
                    },
                )?;
            } else {
                tracing::info!("No result for query {q_id} / {q_uuid}");
            }

            conn.execute(
                "UPDATE queries SET 
                    status = :status, 
                    end_time = :end_time, 
                    duration_ms = :duration_ms, 
                    result_count = :result_count, 
                    result_id = :result_id,
                    error = :error, 
                    diagnostic_error = :diagnostic_error 
                WHERE id = :id",
                named_params! {
                    ":status": q_status,
                    ":end_time": q_end_time,
                    ":duration_ms": q_duration_ms,
                    ":result_count": q_result_count,
                    ":result_id": q_id,
                    ":error": q_error,
                    ":diagnostic_error": q_diagnostic_error,
                    ":id": q_id,
                },
            )
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::QueryUpdateSnafu)?;
        Ok(())
    }

    #[instrument(
        name = "SqliteHistoryStore::get_query",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord> {
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let res = conn
            .interact(move |conn| -> SqlResult<QueryRecord> {
                let mut stmt = conn.prepare(
                    "SELECT
                    q.id,
                    q.worksheet_id,
                    q.query,
                    q.start_time,
                    q.end_time,
                    q.duration_ms,
                    q.result_count,
                    q.status,
                    q.error,
                    q.diagnostic_error,
                    r.result AS result
                FROM queries AS q
                LEFT JOIN results AS r
                    ON q.result_id = r.id
                WHERE q.id = ?1
                ",
                )?;

                // result will be NULL if no corresponding record in results
                stmt.query_row([id.to_string()], |row| {
                    Ok(QueryRecord {
                        id: parse_query_record_id(&row.get::<_, String>(0)?)?,
                        worksheet_id: row.get::<_, Option<i64>>(1)?,
                        query: row.get(2)?,
                        start_time: parse_date(&row.get::<_, String>(3)?)?,
                        end_time: parse_date(&row.get::<_, String>(4)?)?,
                        duration_ms: row.get(5)?,
                        result_count: row.get(6)?,
                        status: row
                            .get::<_, String>(7)?
                            .as_str()
                            .parse::<QueryStatus>()
                            .unwrap_or(QueryStatus::Running),
                        error: row.get(8)?,
                        diagnostic_error: row.get(9)?,
                        result: row.get::<_, Option<String>>(10)?,
                    })
                })
            })
            .await?;

        if res == Err(rusqlite::Error::QueryReturnedNoRows) {
            history_err::QueryNotFoundSnafu { query_id: id }.fail()
        } else {
            res.context(core_utils_err::RuSqliteSnafu)
                .context(history_err::QueryGetSnafu)
        }
    }

    #[instrument(
        name = "SqliteHistoryStore::get_queries",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_queries(&self, _params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
        let conn = self
            .db
            .sqlite_history_store
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let items = conn
            .interact(|conn| -> SqlResult<Vec<QueryRecord>> {
                let mut stmt = conn.prepare(
                    "SELECT id, worksheet_id, query, start_time, end_time,
                        duration_ms, result_count, status, error, diagnostic_error
                 FROM queries",
                )?;

                let rows = stmt.query_map([], |row| {
                    Ok(QueryRecord {
                        id: parse_query_record_id(&row.get::<_, String>(0)?)?,
                        worksheet_id: row.get::<_, Option<i64>>(1)?,
                        query: row.get(2)?,
                        start_time: parse_date(&row.get::<_, String>(3)?)?,
                        end_time: parse_date(&row.get::<_, String>(4)?)?,
                        duration_ms: row.get(5)?,
                        result_count: row.get(6)?,
                        result: None,
                        status: row
                            .get::<_, String>(7)?
                            .as_str()
                            .parse::<QueryStatus>()
                            .unwrap_or(QueryStatus::Running),
                        error: row.get(8)?,
                        diagnostic_error: row.get(9)?,
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

        let res = if query_record.status == QueryStatus::Running {
            self.add_query(query_record).await
        } else {
            self.update_query(query_record).await
        };

        if let Err(err) = res {
            // Record the result as part of the current span.
            tracing::Span::current().record("save_query_history_errror", format!("{err:?}"));

            tracing::error!(error = %err, "Failed to record query history");
        }
    }
}

fn parse_query_record_id(id: &str) -> SqlResult<QueryRecordId> {
    id.parse::<QueryRecordId>().map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn parse_date(date: &str) -> SqlResult<DateTime<Utc>> {
    let res = DateTime::parse_from_rfc3339(date).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(res.with_timezone(&Utc))
}
