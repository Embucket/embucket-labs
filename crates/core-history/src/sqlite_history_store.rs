use crate::ResultSet;
use crate::errors::{self as history_err, Result};
use crate::interface::{GetQueriesParams, HistoryStore};
use crate::{QueryRecord, QueryRecordId, QueryStatus, Worksheet, WorksheetId};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use core_sqlite::SqliteDb;
use core_utils::errors::{self as core_utils_err};
use rusqlite::Result as SqlResult;
use rusqlite::named_params;
use snafu::ResultExt;
use tracing::instrument;

pub const SQLITE_HISTORY_DB_NAME: &str = "sqlite_data/queries.db";
pub const SQLITE_RESULTS_DB_NAME: &str = "sqlite_data/results.db";

const RESULTS_CREATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,                -- using TEXT for timestamp (ISO8601)
    result BLOB NOT NULL,               -- serialized ResultSet
    rows_count INTEGER NOT NULL,
    data_format TEXT NOT NULL
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
    result_id TEXT,                     -- table : results.id
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

pub struct SlateDBHistoryStore {
    pub queries_db: SqliteDb,
    pub results_db: SqliteDb,
}

impl std::fmt::Debug for SlateDBHistoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDBHistoryStore").finish()
    }
}

impl SlateDBHistoryStore {
    #[allow(clippy::expect_used)]
    #[must_use]
    pub async fn new(db: core_utils::Db) -> Self {
        Self {
            queries_db: SqliteDb::new(db.slate_db(), SQLITE_HISTORY_DB_NAME)
                .await
                .expect("Failed to initialize sqlite store"),
            results_db: SqliteDb::new(db.slate_db(), SQLITE_RESULTS_DB_NAME)
                .await
                .expect("Failed to initialize sqlite store"),
        }
    }

    // Create a new store with a new in-memory database
    #[allow(clippy::expect_used)]
    pub async fn new_in_memory() -> Self {
        let utils_db = core_utils::Db::memory().await;

        // use unique filename for every test, create in memory database
        let thread = std::thread::current();
        let thread_name = thread
            .name()
            .map_or("<unnamed>", |s| s.split("::").last().unwrap_or("<unnamed>"));
        let queries_db_name = format!("file:{thread_name}?mode=memory");
        let results_db_name = format!("file:{thread_name}_r?mode=memory");
        let store = Self {
            queries_db: SqliteDb::new(utils_db.slate_db(), &queries_db_name)
                .await
                .expect("Failed to create SqliteDb for queries"),
            results_db: SqliteDb::new(utils_db.slate_db(), &results_db_name)
                .await
                .expect("Failed to create SqliteDb for results"),
        };
        store
            .create_tables()
            .await
            .expect("Failed to create tables");
        store
    }
}

impl SlateDBHistoryStore {
    #[instrument(
        name = "SqliteHistoryStore::create_tables",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    pub async fn create_tables(&self) -> Result<()> {
        let queries_connection = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::CoreUtilsSnafu)?;
        let results_connection = self
            .results_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::CoreUtilsSnafu)?;

        let result = tokio::try_join!(
            queries_connection.interact(|conn| -> SqlResult<usize> {
                conn.execute("BEGIN", [])?;
                conn.execute(WORKSHEETS_CREATE_TABLE, [])?;
                conn.execute(QUERIES_CREATE_TABLE, [])?;
                conn.execute("COMMIT", [])
            }),
            results_connection
                .interact(|conn| -> SqlResult<usize> { conn.execute(RESULTS_CREATE_TABLE, []) }),
        )?;
        result.0.context(history_err::CreateTablesSnafu)?;
        result.1.context(history_err::CreateTablesSnafu)?;

        tracing::Span::current().record("ok", true);
        Ok(())
    }
}

#[async_trait]
impl HistoryStore for SlateDBHistoryStore {
    #[instrument(
        name = "SqliteHistoryStore::add_worksheet",
        level = "debug",
        skip(self, worksheet),
        fields(ok),
        err
    )]
    async fn add_worksheet(&self, worksheet: Worksheet) -> Result<Worksheet> {
        let conn = self
            .queries_db
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
        tracing::Span::current().record("ok", true);
        Ok(worksheet)
    }

    #[instrument(
        name = "SqliteHistoryStore::get_worksheet",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    async fn get_worksheet(&self, id: WorksheetId) -> Result<Worksheet> {
        let conn = self
            .queries_db
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
            tracing::Span::current().record("ok", true);
            Ok(res
                .context(core_utils_err::RuSqliteSnafu)
                .context(history_err::WorksheetGetSnafu)?)
        }
    }

    #[instrument(name = "SqliteHistoryStore::update_worksheet", level = "debug", skip(self, worksheet), fields(ok, id = worksheet.id), err)]
    async fn update_worksheet(&self, mut worksheet: Worksheet) -> Result<()> {
        worksheet.set_updated_at(None); // set current time

        let conn = self
            .queries_db
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

        tracing::Span::current().record("ok", true);
        Ok(())
    }

    #[instrument(
        name = "SqliteHistoryStore::delete_worksheet",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    async fn delete_worksheet(&self, id: WorksheetId) -> Result<()> {
        let conn = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetUpdateSnafu)?;

        let deleted = conn
            .interact(move |conn| -> SqlResult<usize> {
                conn.execute(
                    "DELETE FROM worksheets WHERE id = :id",
                    named_params! { ":id": id },
                )
            })
            .await?
            .context(core_utils_err::RuSqliteSnafu)
            .context(history_err::WorksheetDeleteSnafu)?;

        if deleted == 0 {
            history_err::WorksheetNotFoundSnafu {
                message: id.to_string(),
            }
            .fail()
        } else {
            tracing::Span::current().record("ok", true);
            Ok(())
        }
    }

    #[instrument(
        name = "SqliteHistoryStore::get_worksheets",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    async fn get_worksheets(&self) -> Result<Vec<Worksheet>> {
        let conn = self
            .queries_db
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

        tracing::Span::current().record("ok", true);
        Ok(res)
    }

    #[instrument(
        name = "SqliteHistoryStore::add_query",
        level = "debug",
        skip(self, item),
        fields(ok, item = format!("{item:#?}")),
        err
    )]
    async fn add_query(&self, item: &QueryRecord) -> Result<()> {
        let conn = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let q = item.clone();
        conn.interact(move |conn| -> SqlResult<usize> {
            conn.execute(
                "INSERT INTO queries (
                    id,
                    worksheet_id,
                    result_id,                    
                    query,
                    start_time,
                    end_time,
                    duration_ms,
                    result_count,
                    status,
                    error,
                    diagnostic_error )
                VALUES (
                    :id,
                    :worksheet_id,
                    :result_id,                    
                    :query,
                    :start_time,
                    :end_time,
                    :duration_ms,
                    :result_count,
                    :status,
                    :error,
                    :diagnostic_error
                    )",
                named_params! {
                    ":id": q.id.to_string(),
                    ":worksheet_id": q.worksheet_id,
                    ":result_id": None::<String>,
                    ":query": q.query,
                    ":start_time": q.start_time.to_rfc3339(),
                    ":end_time": q.end_time.to_rfc3339(),
                    ":duration_ms": q.duration_ms,
                    ":result_count": q.result_count,
                    ":status": q.status.to_string(),
                    ":error": q.error,
                    ":diagnostic_error": q.diagnostic_error,
                },
            )
        })
        .await?
        .context(core_utils_err::RuSqliteSnafu)
        .context(history_err::QueryAddSnafu)?;

        tracing::Span::current().record("ok", true);
        Ok(())
    }

    #[instrument(
        name = "SqliteHistoryStore::update_query",
        level = "debug",
        skip(self, item),
        fields(ok),
        err
    )]
    async fn update_query(&self, item: &QueryRecord, result_set: Option<ResultSet>) -> Result<()> {
        let queries_conn = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;
        let results_conn = self
            .results_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::WorksheetAddSnafu)?;

        let q = item.clone();

        let q_uuid = q.id.as_uuid();
        let q_id = q.id.to_string();
        let q_id2 = q.id.to_string();
        let q_status = q.status.to_string();
        let q_end_time = q.end_time.to_rfc3339();
        let q_duration_ms = q.duration_ms;
        let q_result_count = q.result_count;
        let q_error = q.error;
        let q_diagnostic_error = q.diagnostic_error;

        tracing::info!("update_query: {q_id} / {q_uuid} status={q_status}");

        let update_future = queries_conn.interact(move |conn| -> SqlResult<usize> {
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
        });

        let serialized = if let Some(result) = result_set {
            result.as_serialized()
        } else {
            None
        };

        if let Some((serialized_result, serialized_rows_count)) = serialized {
            let res = tokio::try_join!(
                // at first insert result, to satisfy constraint in update stmt
                update_future,
                results_conn.interact(move |conn| -> SqlResult<usize> {
                    conn.execute(
                        "INSERT INTO results (
                            id,
                            result,
                            rows_count,
                            data_format
                        ) VALUES (
                            :id,
                            :result,
                            :rows_count,
                            :data_format
                        )",
                        named_params! {
                            ":id": q_id2,
                            ":result": serialized_result.as_ref(),
                            ":rows_count": serialized_rows_count,
                            ":data_format": "json", // TODO: Support arrow
                        },
                    )
                }),
            )?;
            let updated_queries = res
                .0
                .context(core_utils_err::RuSqliteSnafu)
                .context(history_err::QueryUpdateSnafu)?;
            let inserted_results = res
                .1
                .context(core_utils_err::RuSqliteSnafu)
                .context(history_err::ResultAddSnafu)?;

            tracing::debug!(
                "updated_queries: {updated_queries}, inserted_results: {inserted_results}"
            );
        } else {
            let updated_queries = update_future
                .await?
                .context(core_utils_err::RuSqliteSnafu)
                .context(history_err::QueryUpdateSnafu)?;
            tracing::debug!("updated_queries: {updated_queries}");
        }

        tracing::Span::current().record("ok", true);
        Ok(())
    }

    #[instrument(
        name = "SqliteHistoryStore::get_query",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    async fn get_query(&self, id: QueryRecordId) -> Result<QueryRecord> {
        let conn = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let res = conn
            .interact(move |conn| -> SqlResult<QueryRecord> {
                let mut stmt = conn.prepare(
                    "SELECT
                    id,
                    worksheet_id,
                    result_id,
                    query,
                    start_time,
                    end_time,
                    duration_ms,
                    result_count,
                    status,
                    error,
                    diagnostic_error
                FROM queries
                WHERE id = ?1",
                )?;

                stmt.query_row([id.to_string()], |row| {
                    Ok(QueryRecord {
                        id: parse_query_record_id(&row.get::<_, String>(0)?)?,
                        worksheet_id: row.get::<_, Option<i64>>(1)?,
                        result_id: row.get::<_, Option<String>>(2)?,
                        query: row.get(3)?,
                        start_time: parse_date(&row.get::<_, String>(4)?)?,
                        end_time: parse_date(&row.get::<_, String>(5)?)?,
                        duration_ms: row.get(6)?,
                        result_count: row.get(7)?,
                        status: row
                            .get::<_, String>(8)?
                            .as_str()
                            .parse::<QueryStatus>()
                            .unwrap_or(QueryStatus::Running),
                        error: row.get(9)?,
                        diagnostic_error: row.get(10)?,
                    })
                })
            })
            .await?;

        if res == Err(rusqlite::Error::QueryReturnedNoRows) {
            history_err::QueryNotFoundSnafu { query_id: id }.fail()
        } else {
            tracing::Span::current().record("ok", true);
            Ok(res
                .context(core_utils_err::RuSqliteSnafu)
                .context(history_err::QueryGetSnafu)?)
        }
    }

    #[instrument(
        name = "SqliteHistoryStore::get_queries",
        level = "debug",
        skip(self),
        fields(ok),
        err
    )]
    async fn get_queries(&self, params: GetQueriesParams) -> Result<Vec<QueryRecord>> {
        let GetQueriesParams {
            worksheet_id,
            cursor,
            limit,
            ..
        } = params;

        let conn = self
            .queries_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let items = conn
            .interact(move |conn| -> SqlResult<Vec<QueryRecord>> {
                let mut stmt = conn.prepare(&format!(
                    "SELECT
                        id,
                        worksheet_id,
                        result_id,
                        query,
                        start_time,
                        end_time,
                        duration_ms,
                        result_count,
                        status,
                        error,
                        diagnostic_error
                    FROM queries
                    WHERE {} id > :cursor
                    ORDER BY start_time DESC
                    LIMIT :limit",
                    worksheet_id
                        .map_or_else(String::new, |wid| format!("worksheet_id = {wid} AND"))
                ))?;
                // Use simplified offset model, as we can't return correct cursor in UI

                let rows = stmt.query_map(
                    named_params! {
                        ":cursor": cursor.unwrap_or_default().to_string(),
                        ":limit": limit.map_or(-1, i32::from), // -1 means no uuper limit
                    },
                    |row| {
                        Ok(QueryRecord {
                            id: parse_query_record_id(&row.get::<_, String>(0)?)?,
                            worksheet_id: row.get::<_, Option<i64>>(1)?,
                            result_id: row.get::<_, Option<String>>(2)?,
                            query: row.get(3)?,
                            start_time: parse_date(&row.get::<_, String>(4)?)?,
                            end_time: parse_date(&row.get::<_, String>(5)?)?,
                            duration_ms: row.get(6)?,
                            result_count: row.get(7)?,
                            status: row
                                .get::<_, String>(8)?
                                .as_str()
                                .parse::<QueryStatus>()
                                .unwrap_or(QueryStatus::Running),
                            error: row.get(9)?,
                            diagnostic_error: row.get(10)?,
                        })
                    },
                )?;

                let mut results = Vec::new();
                for ws in rows {
                    results.push(ws?);
                }

                Ok(results)
            })
            .await?
            .context(core_utils_err::RuSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        tracing::Span::current().record("ok", true);
        Ok(items)
    }

    fn new_query_record(&self, query: &str, worksheet_id: Option<WorksheetId>) -> QueryRecord {
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
    async fn save_query_record(&self, query_record: &QueryRecord, result_set: Option<ResultSet>) {
        // This function won't fail, just sends happened write errors to the logs

        let res = if query_record.status == QueryStatus::Running {
            self.add_query(query_record).await
        } else {
            self.update_query(query_record, result_set).await
        };

        if let Err(err) = res {
            // Record the result as part of the current span.
            tracing::Span::current().record("save_query_history_errror", format!("{err:?}"));

            tracing::error!(error = %err, "Failed to record query history");
        }
    }

    #[instrument(
        name = "SlateDBSqliteHistoryStore::get_query_result",
        skip(self),
        fields(ok, rows_count, data_format)
    )]
    async fn get_query_result(&self, id: QueryRecordId) -> Result<ResultSet> {
        let conn = self
            .results_db
            .conn()
            .await
            .context(core_utils_err::CoreSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        let (rows_count, raw_result, data_format) = conn
            .interact(move |conn| -> SqlResult<(i64, Bytes, String)> {
                let mut stmt = conn.prepare(
                    "SELECT
                    rows_count,
                    result,
                    data_format
                FROM results
                WHERE id = ?1",
                )?;

                // result will be NULL if no corresponding record in results
                stmt.query_row([id.to_string()], |row| {
                    let rows_count = row.get::<_, i64>(0)?;
                    let raw_result = Bytes::from(row.get::<_, Vec<u8>>(1)?);
                    let data_format = row.get::<_, String>(2)?;

                    Ok((rows_count, raw_result, data_format))
                })
            })
            .await?
            .context(core_utils_err::RuSqliteSnafu)
            .context(history_err::QueryGetSnafu)?;

        tracing::Span::current()
            .record("rows_count", rows_count)
            .record("data_format", data_format)
            .record("ok", true);

        ResultSet::try_from(raw_result)
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
