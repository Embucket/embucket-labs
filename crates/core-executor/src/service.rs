use bytes::{Buf, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::csv::reader::Format;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::execution::DiskManager;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_common::TableReference;
use snafu::{OptionExt, ResultExt};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::vec;
use std::{collections::HashMap, sync::Arc};
use time::{Duration as DateTimeDuration, OffsetDateTime};

use super::error::{self as ex_error, Result};
use super::models::{AsyncQueryHandle, QueryContext, QueryResult, QueryResultStatus};
use super::session::UserSession;
use crate::session::{SESSION_INACTIVITY_EXPIRATION_SECONDS, to_unix};
use crate::utils::{Config, MemPoolType, query_result_to_history};
use core_history::history_store::HistoryStore;
use core_history::store::SlateDBHistoryStore;
use core_history::{QueryRecordId, QueryStatus};
use core_metastore::{Metastore, SlateDBMetastore, TableIdent as MetastoreTableIdent};
use core_utils::Db;
use dashmap::DashMap;
use df_catalog::catalog_list::EmbucketCatalogList;
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
    async fn submit_query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<AsyncQueryHandle>;
    async fn cancel_query(&self, query_id: QueryRecordId) -> Result<()>;
    async fn wait_async_query_completion(
        &self,
        query_handle: AsyncQueryHandle,
    ) -> Result<QueryResult>;
    async fn query_result(&self, query_id: QueryRecordId) -> Result<QueryResult>;
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
    pub queries: Arc<DashMap<i64, CancellationToken>>,
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
        let catalog_list = Self::catalog_list(metastore.clone(), history_store.clone()).await?;
        let runtime_env = Self::runtime_env(&config, catalog_list.clone())?;
        Ok(Self {
            metastore,
            history_store,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            catalog_list,
            runtime_env,
            queries: Arc::new(DashMap::new()),
        })
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

            let disk_manager = DiskManager::try_new(DiskManagerConfig::NewOs)
                .context(ex_error::DataFusionSnafu)?;

            let disk_manager = Arc::try_unwrap(disk_manager)
                .ok()
                .context(ex_error::DataFusionDiskManagerSnafu)?
                .with_max_temp_directory_size(disk_limit_bytes)
                .context(ex_error::DataFusionSnafu)?;

            let disk_config = DiskManagerConfig::new_existing(Arc::new(disk_manager));
            rt_builder = rt_builder.with_disk_manager(disk_config);
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
        level = "debug",
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
            .context(ex_error::MissingDataFusionSessionSnafu { id: session_id })?;
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
        name = "ExecutionService::cancel_query",
        level = "debug",
        skip(self),
        err
    )]
    async fn cancel_query(&self, query_id: QueryRecordId) -> Result<()> {
        let cancel_token =
            self.queries
                .get(&query_id.into())
                .context(ex_error::QueryIsntRunningSnafu {
                    query_id: query_id.to_string(),
                })?;
        cancel_token.cancel();
        Ok(())
    }

    #[tracing::instrument(
        name = "ExecutionService::wait_async_query_completion",
        level = "debug",
        skip(self, query_handle),
        fields(query_id = query_handle.query_id.as_i64(), query_uuid = query_handle.query_id.to_uuid().to_string()),
        err
    )]
    async fn wait_async_query_completion(
        &self,
        query_handle: AsyncQueryHandle,
    ) -> Result<QueryResult> {
        let _ = self.queries.get(&query_handle.query_id.into()).context(
            ex_error::QueryIsntRunningSnafu {
                query_id: query_handle.query_id.to_string(),
            },
        )?;

        let query_status = query_handle
            .rx
            .await
            .context(ex_error::QueryResultRecvSnafu)?;

        Ok(query_status.query_result?)
    }

    #[tracing::instrument(
        name = "ExecutionService::query_result",
        level = "debug",
        skip(self),
        err
    )]
    async fn query_result(&self, query_id: QueryRecordId) -> Result<QueryResult> {
        let query_record_res = self
            .history_store
            .get_query(query_id)
            .await
            .context(ex_error::QueryHistorySnafu);

        let query_record = query_record_res.context(ex_error::QueryExecutionSnafu {
            query_id: query_id.to_string(),
        })?;

        Ok(query_record
            .try_into()
            .context(ex_error::QueryExecutionSnafu {
                query_id: query_id.to_string(),
            })?)
    }

    #[tracing::instrument(
        name = "ExecutionService::async_query",
        level = "debug",
        skip(self),
        fields(query_id, query_uuid, old_queries_count = self.queries.len()),
        err
    )]
    async fn submit_query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<AsyncQueryHandle> {
        let user_session = self.get_session(session_id).await?;

        if self.queries.len() >= self.config.max_concurrency_level {
            return ex_error::ConcurrencyLimitSnafu.fail();
        }

        let mut history_record = self
            .history_store
            .query_record(query, query_context.worksheet_id);

        let query_id = history_record.query_id();

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("query_id", query_id.as_i64())
            .record("query_uuid", query_id.to_uuid().to_string());

        // Attach the generated query ID to the query context before execution.
        // This ensures consistent tracking and logging of the query across all layers.
        let query_obj = user_session.query(query, query_context.with_query_id(query_id));

        // add cancellation token to the map, so it can be cancelled if needed
        // also presence in this map means that query is running
        let cancel_token = CancellationToken::new();
        self.queries.insert(query_id.into(), cancel_token.clone());
        // eprintln!("submit_query: {query_id}, {}, {}", self.queries.len(), chrono::Utc::now());

        let query_timeout_secs = self.config.query_timeout_secs;

        let history_store_ref = self.history_store.clone();
        let queries_ref = self.queries.clone();

        let (tx, rx) = oneshot::channel();

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
                            QueryResultStatus {
                                query_result: inner_result.context(ex_error::QueryExecutionSnafu {
                                    query_id: query_id.to_string(),
                                }),
                                status: QueryStatus::Successful,
                            }
                        },
                        Err(_) => {
                            QueryResultStatus {
                                query_result: Err(ex_error::QueryTimeoutSnafu.build()),
                                status: QueryStatus::TimedOut,
                            }
                        },
                    }
                },
                () = cancel_token.cancelled() => {
                    QueryResultStatus {
                        query_result: Err(ex_error::QueryCancelledSnafu { query_id: query_id.to_string() }.build()),
                        status: QueryStatus::Canceled,
                    }
                }
            };

            let _ = tracing::debug_span!(
                "ExecutionService::submit_query_result",
                query_id = query_id.as_i64(),
                query_uuid = query_id.to_uuid().to_string(),
                query_status = format!("{:?}", query_result_status.status),
            )
            .entered();

            // Record the query in the session’s history, including result count or error message.
            // This ensures all queries are traceable and auditable within a session, which enables
            // features like `last_query_id()` and enhances debugging and observability.
            history_store_ref
                .save_query_record(
                    &mut history_record,
                    query_result_to_history(&query_result_status.query_result),
                )
                .await;

            // save result to the query histore before transfering ownership to the channel
            let _ = tx.send(query_result_status);

            // cleanup: remove from map when done
            queries_ref.remove(&query_id.into());
        });

        Ok(AsyncQueryHandle { query_id, rx })
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
        self.wait_async_query_completion(query_handle).await
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
