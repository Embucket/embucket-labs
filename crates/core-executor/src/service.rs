use bytes::{Buf, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::csv::reader::Format;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::execution::DiskManager;
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_common::TableReference;
use snafu::ResultExt;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::vec;
use std::{collections::HashMap, sync::Arc};
use time::{Duration as DateTimeDuration, OffsetDateTime};

use super::error::{self as ex_error, Result};
use super::models::{AsyncQueryHandle, QueryContext, QueryResult, QueryResultStatus};
use super::running_queries::{RunningQueries, RunningQueriesRegistry, RunningQuery};
use super::session::UserSession;
use crate::running_queries::AbortQuery;
use crate::session::{SESSION_INACTIVITY_EXPIRATION_SECONDS, to_unix};
use crate::utils::{Config, MemPoolType};
use core_history::history_store::HistoryStore;
use core_history::store::SlateDBHistoryStore;
use core_history::{QueryRecordId, QueryStatus};
use core_metastore::{
    Database, Metastore, Schema, SchemaIdent, SlateDBMetastore, TableIdent as MetastoreTableIdent,
    Volume, VolumeType,
};
use core_utils::Db;
use df_catalog::catalog_list::{DEFAULT_CATALOG, EmbucketCatalogList};
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};
use tracing::Instrument;
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "public";

#[async_trait::async_trait]
pub trait ExecutionService: Send + Sync {
    async fn create_session(&self, session_id: &str) -> Result<Arc<UserSession>>;
    async fn update_session_expiry(&self, session_id: &str) -> Result<bool>;
    async fn delete_expired_sessions(&self) -> Result<()>;
    async fn get_session(&self, session_id: &str) -> Result<Arc<UserSession>>;
    async fn session_exists(&self, session_id: &str) -> bool;
    // Currently delete_session function is not used
    // async fn delete_session(&self, session_id: String) -> Result<()>;
    fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>>;

    /// Aborts a query by `query_id` or `request_id`.
    ///
    /// # Arguments
    ///
    /// * `abort_query` - The query to abort. Provided either `query_id` or `request_id` and `sql_text`.
    ///
    /// # Returns
    ///
    /// A `Result` of type `()`. The `Ok` variant contains an empty tuple,
    /// and the `Err` variant contains an `Error`.
    fn abort_query(&self, abort_query: AbortQuery) -> Result<()>;

    /// Submits a query to be executed asynchronously. Query result can be consumed with
    /// `wait_submitted_query_result`.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The ID of the user session.
    /// * `query` - The SQL query to be executed.
    /// * `query_context` - The context of the query execution.
    ///
    /// # Returns
    ///
    /// A `Result` of type `AsyncQueryHandle`. The `Ok` variant contains the query handle,
    /// to be used with `wait_submitted_query_result`. The `Err` variant contains submission `Error`.
    async fn submit_query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<AsyncQueryHandle>;

    /// Wait while sabmitted query finished, it returns query result or real context rich error
    /// # Arguments
    ///
    /// * `query_handle` - The handle of the submitted query.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryResult`. The `Ok` variant contains the query result,
    /// and the `Err` variant contains a real context rich error.
    async fn wait_submitted_query_result(
        &self,
        query_handle: AsyncQueryHandle,
    ) -> Result<QueryResult>;

    /// Wait while any running query finished, it returns query result loaded from query history
    /// or error which is just a simple wrapper around stringified error loaded from history
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query to wait for.
    ///
    /// # Returns
    ///
    /// A `Result` of type `Result<QueryResult>`. The `Ok` variant contains nested Result loaded from
    /// query history including query result / error. The `Err` variant is a simple error wrapper
    /// `Error::QueryExecution` with `query_id`.
    async fn wait_historical_query_result(
        &self,
        query_id: QueryRecordId,
    ) -> Result<Result<QueryResult>>;

    /// Synchronously executes a query and returns the result.
    /// It is a wrapper around `submit_query` and `wait_submitted_query_result`.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The ID of the user session.
    /// * `query` - The SQL query to be executed.
    /// * `query_context` - The context of the query execution.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryResult`. The `Ok` variant contains the query result,
    /// and the `Err` variant contains a real context rich error.
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryResult>;

    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> Result<usize>;
}

pub struct CoreExecutionService {
    metastore: Arc<dyn Metastore>,
    history_store: Arc<dyn HistoryStore>,
    df_sessions: Arc<RwLock<HashMap<String, Arc<UserSession>>>>,
    config: Arc<Config>,
    catalog_list: Arc<EmbucketCatalogList>,
    runtime_env: Arc<RuntimeEnv>,
    queries: Arc<RunningQueriesRegistry>,
}

impl CoreExecutionService {
    #[tracing::instrument(
        name = "CoreExecutionService::new",
        level = "debug",
        skip(metastore, history_store, config),
        err
    )]
    pub async fn new(
        metastore: Arc<dyn Metastore>,
        history_store: Arc<dyn HistoryStore>,
        config: Arc<Config>,
    ) -> Result<Self> {
        if config.bootstrap_default_entities {
            // do not fail on bootstrap errors
            let _ = Self::bootstrap(metastore.clone()).await;
        }

        let catalog_list = Self::catalog_list(metastore.clone(), history_store.clone()).await?;
        let runtime_env = Self::runtime_env(&config, catalog_list.clone())?;
        Ok(Self {
            metastore,
            history_store,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            catalog_list,
            runtime_env,
            queries: Arc::new(RunningQueriesRegistry::new()),
        })
    }

    ///This function bootstraps the service if no flag is present (`--no-bootstrap`) with:
    /// 1. Creation of a default in-memory volume named `embucket`
    /// 2. Creation of a default database `embucket` in the volume `embucket`
    /// 3. Creation of a default schema `public` in the database `embucket`
    ///
    /// Only traces the errors, doesn't panic.
    #[tracing::instrument(
        name = "CoreExecutionService::bootstrap",
        level = "info",
        skip(metastore),
        err
    )]
    #[allow(clippy::cognitive_complexity)]
    async fn bootstrap(metastore: Arc<dyn Metastore>) -> Result<()> {
        let ident = DEFAULT_CATALOG.to_string();
        metastore
            .create_volume(&ident, Volume::new(ident.clone(), VolumeType::Memory))
            .await
            .context(ex_error::BootstrapSnafu {
                entity_type: "volume",
            })?;

        metastore
            .create_database(
                &ident,
                Database {
                    ident: ident.clone(),
                    properties: None,
                    volume: ident.clone(),
                },
            )
            .await
            .context(ex_error::BootstrapSnafu {
                entity_type: "database",
            })?;

        let schema_ident = SchemaIdent::new(ident.clone(), DEFAULT_SCHEMA.to_string());
        metastore
            .create_schema(
                &schema_ident,
                Schema {
                    ident: schema_ident.clone(),
                    properties: None,
                },
            )
            .await
            .context(ex_error::BootstrapSnafu {
                entity_type: "schema",
            })?;

        Ok(())
    }

    #[tracing::instrument(
        name = "CoreExecutionService::catalog_list",
        level = "debug",
        skip(metastore, history_store),
        err
    )]
    pub async fn catalog_list(
        metastore: Arc<dyn Metastore>,
        history_store: Arc<dyn HistoryStore>,
    ) -> Result<Arc<EmbucketCatalogList>> {
        let catalog_list = Arc::new(EmbucketCatalogList::new(
            metastore.clone(),
            history_store.clone(),
        ));
        catalog_list
            .register_catalogs()
            .await
            .context(ex_error::RegisterCatalogSnafu)?;
        catalog_list
            .refresh()
            .await
            .context(ex_error::RefreshCatalogListSnafu)?;
        catalog_list
            .clone()
            .start_refresh_internal_catalogs_task(10);
        Ok(catalog_list)
    }

    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn runtime_env(
        config: &Config,
        catalog_list: Arc<EmbucketCatalogList>,
    ) -> Result<Arc<RuntimeEnv>> {
        let mut rt_builder = RuntimeEnvBuilder::new().with_object_store_registry(catalog_list);

        if let Some(memory_limit_mb) = config.mem_pool_size_mb {
            const NUM_TRACKED_CONSUMERS: usize = 5;

            // set memory pool type
            let memory_limit = memory_limit_mb * 1024 * 1024;
            let enable_track = config.mem_enable_track_consumers_pool.unwrap_or(false);

            let memory_pool: Arc<dyn MemoryPool> = match config.mem_pool_type {
                MemPoolType::Fair => {
                    let pool = FairSpillPool::new(memory_limit);
                    if enable_track {
                        Arc::new(TrackConsumersPool::new(
                            pool,
                            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                        ))
                    } else {
                        Arc::new(FairSpillPool::new(memory_limit))
                    }
                }
                MemPoolType::Greedy => {
                    let pool = GreedyMemoryPool::new(memory_limit);
                    if enable_track {
                        Arc::new(TrackConsumersPool::new(
                            pool,
                            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                        ))
                    } else {
                        Arc::new(GreedyMemoryPool::new(memory_limit))
                    }
                }
            };
            rt_builder = rt_builder.with_memory_pool(memory_pool);
        }

        // set disk limit
        if let Some(disk_limit) = config.disk_pool_size_mb {
            let disk_limit_bytes = (disk_limit as u64) * 1024 * 1024;
            let disk_builder = DiskManager::builder()
                .with_mode(DiskManagerMode::OsTmpDirectory)
                .with_max_temp_directory_size(disk_limit_bytes);
            rt_builder = rt_builder.with_disk_manager_builder(disk_builder);
        }

        rt_builder.build_arc().context(ex_error::DataFusionSnafu)
    }
}

#[async_trait::async_trait]
impl ExecutionService for CoreExecutionService {
    #[tracing::instrument(
        name = "ExecutionService::create_session",
        level = "debug",
        skip(self),
        fields(new_sessions_count),
        err
    )]
    async fn create_session(&self, session_id: &str) -> Result<Arc<UserSession>> {
        {
            let sessions = self.df_sessions.read().await;
            if let Some(session) = sessions.get(session_id) {
                return Ok(session.clone());
            }
        }
        let user_session: Arc<UserSession> = Arc::new(UserSession::new(
            self.metastore.clone(),
            self.history_store.clone(),
            self.queries.clone(),
            self.config.clone(),
            self.catalog_list.clone(),
            self.runtime_env.clone(),
        )?);
        {
            tracing::trace!("Acquiring write lock for df_sessions");
            let mut sessions = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            sessions.insert(session_id.to_string(), user_session.clone());

            // Record the result as part of the current span.
            tracing::Span::current().record("new_sessions_count", sessions.len());
        }
        Ok(user_session)
    }

    #[tracing::instrument(
        name = "ExecutionService::update_session_expiry",
        level = "debug",
        skip(self),
        fields(old_sessions_count, new_sessions_count, now),
        err
    )]
    async fn update_session_expiry(&self, session_id: &str) -> Result<bool> {
        let mut sessions = self.df_sessions.write().await;

        let res = if let Some(session) = sessions.get_mut(session_id) {
            let now = OffsetDateTime::now_utc();
            let new_expiry =
                to_unix(now + DateTimeDuration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS));
            session.expiry.store(new_expiry, Ordering::Relaxed);

            // Record the result as part of the current span.
            tracing::Span::current().record("sessions_count", sessions.len());
            true
        } else {
            false
        };
        Ok(res)
    }

    #[tracing::instrument(
        name = "ExecutionService::delete_expired_sessions",
        level = "trace",
        skip(self),
        fields(old_sessions_count, new_sessions_count, now),
        err
    )]
    async fn delete_expired_sessions(&self) -> Result<()> {
        let now = to_unix(OffsetDateTime::now_utc());
        let mut sessions = self.df_sessions.write().await;

        let old_sessions_count = sessions.len();

        sessions.retain(|session_id, session| {
            let expiry = session.expiry.load(Ordering::Relaxed);
            if expiry <= now {
                let _ = tracing::debug_span!(
                    "ExecutionService::delete_expired_session",
                    session_id,
                    expiry,
                    now
                )
                .entered();
                false
            } else {
                true
            }
        });

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("old_sessions_count", old_sessions_count)
            .record("new_sessions_count", sessions.len())
            .record("now", now);
        Ok(())
    }

    #[tracing::instrument(
        name = "ExecutionService::get_session",
        level = "debug",
        skip(self),
        fields(session_id),
        err
    )]
    async fn get_session(&self, session_id: &str) -> Result<Arc<UserSession>> {
        let sessions = self.df_sessions.read().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| ex_error::MissingDataFusionSessionSnafu { id: session_id }.build())?;
        Ok(session.clone())
    }

    #[tracing::instrument(
        name = "ExecutionService::session_exists",
        level = "debug",
        skip(self),
        fields(session_id)
    )]
    async fn session_exists(&self, session_id: &str) -> bool {
        let sessions = self.df_sessions.read().await;
        sessions.contains_key(session_id)
    }

    // #[tracing::instrument(
    //     name = "ExecutionService::delete_session",
    //     level = "debug",
    //     skip(self),
    //     fields(new_sessions_count),
    //     err
    // )]
    // async fn delete_session(&self, session_id: String) -> Result<()> {
    //     // TODO: Need to have a timeout for the lock
    //     let mut session_list = self.df_sessions.write().await;
    //     session_list.remove(&session_id);

    //     // Record the result as part of the current span.
    //     tracing::Span::current().record("new_sessions_count", session_list.len());
    //     Ok(())
    // }
    fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>> {
        self.df_sessions.clone()
    }

    #[tracing::instrument(
        name = "ExecutionService::query",
        level = "debug",
        skip(self),
        fields(query_id),
        err
    )]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryResult> {
        let query_handle = self.submit_query(session_id, query, query_context).await?;
        self.wait_submitted_query_result(query_handle).await
    }

    #[tracing::instrument(
        name = "ExecutionService::wait_submitted_query_result",
        level = "debug",
        skip(self, query_handle),
        fields(query_id = query_handle.query_id.as_i64(), query_uuid = query_handle.query_id.as_uuid().to_string()),
        err
    )]
    async fn wait_submitted_query_result(
        &self,
        query_handle: AsyncQueryHandle,
    ) -> Result<QueryResult> {
        let query_id = query_handle.query_id;
        if !self.queries.is_running(query_id) {
            return ex_error::QueryIsntRunningSnafu { query_id }.fail();
        }

        let recv_result = query_handle.rx.await;
        // do some handling on result recv error
        if recv_result.is_err() {
            // just in case, log error in this way
            tracing::error_span!(
                "error_receiving_query_result_status",
                query_id = query_id.as_i64(),
                query_uuid = query_id.as_uuid().to_string(),
            );
        }

        let query_result_status =
            recv_result.context(ex_error::QueryResultRecvSnafu { query_id })?;

        Ok(query_result_status.query_result?)
    }

    #[tracing::instrument(
        name = "ExecutionService::query_result",
        level = "debug",
        skip(self),
        fields(query_status, query_uuid = query_id.as_uuid().to_string(), running_queries_count = self.queries.count()),
        err
    )]
    async fn wait_historical_query_result(
        &self,
        query_id: QueryRecordId,
    ) -> Result<Result<QueryResult>> {
        if let Ok(mut running_query) = self.queries.get(query_id) {
            let query_status = running_query
                .recv_query_finished()
                .await
                .context(ex_error::QueryStatusRecvSnafu { query_id })?;
            tracing::Span::current().record("query_status", query_status.to_string());
        }

        // query is not running or not running anymore, get the result from history
        let query_record = self
            .history_store
            .get_query(query_id)
            .await
            .context(ex_error::QueryHistorySnafu)
            .context(ex_error::QueryExecutionSnafu { query_id })?;

        if query_record.status == QueryStatus::Running {
            ex_error::QueryIsRunningSnafu { query_id }
                .fail()
                .context(ex_error::QueryExecutionSnafu { query_id })
        } else {
            let fetched_query_result: Result<QueryResult> = if query_record.error.is_some() {
                // convert saved query error to result
                Err(query_record
                    .try_into()
                    .context(ex_error::QueryExecutionSnafu { query_id })?)
            } else {
                // convert saved ResultSet to result
                Ok(query_record
                    .try_into()
                    .context(ex_error::QueryExecutionSnafu { query_id })?)
            };
            Ok(fetched_query_result)
        }
    }

    #[tracing::instrument(
        name = "ExecutionService::abort_query",
        level = "debug",
        skip(self),
        fields(old_queries_count = self.queries.count()),
        err
    )]
    fn abort_query(&self, abort_query: AbortQuery) -> Result<()> {
        self.queries.abort(abort_query)
    }

    #[tracing::instrument(
        name = "ExecutionService::submit_query",
        level = "debug",
        skip(self),
        fields(query_id, query_uuid, old_queries_count = self.queries.count()),
        err
    )]
    async fn submit_query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<AsyncQueryHandle> {
        let user_session = self.get_session(session_id).await?;

        if self.queries.count() >= self.config.max_concurrency_level {
            return ex_error::ConcurrencyLimitSnafu.fail();
        }

        let mut history_record = self
            .history_store
            .query_record(query, query_context.worksheet_id);
        history_record.set_status(QueryStatus::Running);

        let query_id = history_record.query_id();

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("query_id", query_id.as_i64())
            .record("query_uuid", query_id.as_uuid().to_string());

        // Create RunningQuery before query execution as query_context is then transfered to the query object
        let running_query = if let Some(request_id) = &query_context.request_id {
            RunningQuery::new(query_id).with_request_id(*request_id)
        } else {
            RunningQuery::new(query_id)
        };

        // Attach the generated query ID to the query context before execution.
        // This ensures consistent tracking and logging of the query across all layers.
        let query_obj = user_session.query(query, query_context.with_query_id(query_id));

        let cancel_token = self.queries.add(running_query);

        // Add query to history with status: Running
        self.history_store
            .save_query_record(&mut history_record)
            .await;

        let query_timeout_secs = self.config.query_timeout_secs;
        let query_history_rows_limit = self.config.query_history_rows_limit;

        let history_store_ref = self.history_store.clone();
        let queries_ref = self.queries.clone();

        let (tx, rx) = oneshot::channel();

        let child = tracing::info_span!("spawn_query_task");

        let alloc_span = tracing::info_span!(
            target: "alloc",
            "query_alloc",
            query_id = %history_record.query_id(),
            session_id = %session_id
        );
        tokio::spawn(async move {
            let mut query_obj = query_obj;
            // Execute the query with a timeout to prevent long-running or stuck queries
            // from blocking system resources indefinitely. If the timeout is exceeded,
            // convert the timeout into a standard QueryTimeout error so it can be handled
            // and recorded like any other execution failure.
            let result_fut = timeout(Duration::from_secs(query_timeout_secs), query_obj.execute());

            // wait for any future to be resolved
            let query_result_status = tokio::select! {
                finished = result_fut => {
                    match finished {
                        Ok(inner_result) => {
                            // set query execution status to successful or failed
                            let status = inner_result.as_ref().map_or_else(|_| QueryStatus::Failed, |_| QueryStatus::Successful);
                            QueryResultStatus {
                                query_result: inner_result.context(ex_error::QueryExecutionSnafu {
                                    query_id,
                                }),
                                status,
                            }
                        },
                        Err(_) => {
                            QueryResultStatus {
                                query_result: ex_error::QueryTimeoutSnafu.fail().context(ex_error::QueryExecutionSnafu {
                                    query_id,
                                }),
                                status: QueryStatus::TimedOut,
                            }
                        },
                    }
                },
                () = cancel_token.cancelled() => {
                    QueryResultStatus {
                        query_result: ex_error::QueryCancelledSnafu { query_id }.fail().context(ex_error::QueryExecutionSnafu {
                            query_id,
                        }),
                        status: QueryStatus::Canceled,
                    }
                }
            };

            let query_status = query_result_status.status.clone();

            history_record.set_result(&query_result_status.as_historical_result_set(), query_history_rows_limit);
            history_record.set_status(query_status.clone());

            let _ = tracing::debug_span!("spawned_query_task_result",
                query_id = query_id.as_i64(),
                query_uuid = query_id.as_uuid().to_string(),
                query_status = format!("{:?}", query_status),
            )
            .entered();

            // Record the query in the session’s history, including result count or error message.
            // This ensures all queries are traceable and auditable within a session, which enables
            // features like `last_query_id()` and enhances debugging and observability.
            history_store_ref.save_query_record(&mut history_record).await;

            // remove query from running queries registry
            let running_query = queries_ref.remove(query_id);

            // Send result to the result owner
            if tx.send(query_result_status).is_err() {
                // Error happens if receiver is dropped
                // (natural in case if query submitted result owner doesn't listen)
                tracing::error_span!("no_receiver_on_query_result_status",
                    query_id = query_id.as_i64(),
                    query_uuid = query_id.as_uuid().to_string(),
                );
            }

            // notify listeners that historical result is ready
            if let Ok(running_query) = running_query {
                let _ = running_query.notify_query_finished(query_status.clone());
            }
        }.instrument(alloc_span).instrument(child));

        // return handle of the query we just submit
        Ok(AsyncQueryHandle { query_id, rx })
    }

    #[tracing::instrument(
        name = "ExecutionService::upload_data_to_table",
        level = "debug",
        skip(self, data),
        err,
        ret
    )]
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> Result<usize> {
        // TODO: is there a way to avoid temp table approach altogether?
        // File upload works as follows:
        // 1. Convert incoming data to a record batch
        // 2. Create a temporary table in memory
        // 3. Use Execution service to insert data into the target table from the temporary table
        // 4. Drop the temporary table

        // use unique name to support simultaneous uploads
        let unique_id = Uuid::new_v4().to_string().replace('-', "_");
        let user_session = {
            let sessions = self.df_sessions.read().await;
            sessions
                .get(session_id)
                .ok_or_else(|| {
                    ex_error::MissingDataFusionSessionSnafu {
                        id: session_id.to_string(),
                    }
                    .build()
                })?
                .clone()
        };

        let source_table =
            TableReference::full("tmp_db", "tmp_schema", format!("tmp_table_{unique_id}"));
        let target_table = TableReference::full(
            table_ident.database.clone(),
            table_ident.schema.clone(),
            table_ident.table.clone(),
        );
        let inmem_catalog = MemoryCatalogProvider::new();
        inmem_catalog
            .register_schema(
                source_table.schema().unwrap_or_default(),
                Arc::new(MemorySchemaProvider::new()),
            )
            .context(ex_error::DataFusionSnafu)?;
        user_session.ctx.register_catalog(
            source_table.catalog().unwrap_or_default(),
            Arc::new(inmem_catalog),
        );
        // If target table already exists, we need to insert into it
        // otherwise, we need to create it
        let exists = user_session
            .ctx
            .table_exist(target_table.clone())
            .context(ex_error::DataFusionSnafu)?;

        let schema = if exists {
            let table = user_session
                .ctx
                .table(target_table)
                .await
                .context(ex_error::DataFusionSnafu)?;
            table.schema().as_arrow().to_owned()
        } else {
            let (schema, _) = format
                .infer_schema(data.clone().reader(), None)
                .context(ex_error::ArrowSnafu)?;
            schema
        };
        let schema = Arc::new(schema);

        // Here we create an arrow CSV reader that infers the schema from the entire dataset
        // (as `None` is passed for the number of rows) and then builds a record batch
        // TODO: This partially duplicates what Datafusion does with `CsvFormat::infer_schema`
        let csv = ReaderBuilder::new(schema.clone())
            .with_format(format)
            .build_buffered(data.reader())
            .context(ex_error::ArrowSnafu)?;

        let batches: std::result::Result<Vec<_>, _> = csv.collect();
        let batches = batches.context(ex_error::ArrowSnafu)?;

        let rows_loaded = batches
            .iter()
            .map(|batch: &RecordBatch| batch.num_rows())
            .sum();

        let table = MemTable::try_new(schema, vec![batches]).context(ex_error::DataFusionSnafu)?;
        user_session
            .ctx
            .register_table(source_table.clone(), Arc::new(table))
            .context(ex_error::DataFusionSnafu)?;

        let table = source_table.clone();
        let query = if exists {
            format!("INSERT INTO {table_ident} SELECT * FROM {table}")
        } else {
            format!("CREATE TABLE {table_ident} AS SELECT * FROM {table}")
        };

        let mut query = user_session.query(&query, QueryContext::default());
        Box::pin(query.execute()).await?;

        user_session
            .ctx
            .deregister_table(source_table)
            .context(ex_error::DataFusionSnafu)?;

        Ok(rows_loaded)
    }
}

//Test environment
#[allow(clippy::expect_used)]
pub async fn make_test_execution_svc() -> Arc<CoreExecutionService> {
    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    Arc::new(
        CoreExecutionService::new(metastore, history_store, Arc::new(Config::default()))
            .await
            .expect("Failed to create a execution service"),
    )
}
