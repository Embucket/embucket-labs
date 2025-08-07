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
use std::vec;
use std::{collections::HashMap, sync::Arc};

use super::error::{self as ex_error, Result};
use super::{models::QueryContext, models::QueryResult, session::UserSession};
use crate::utils::{Config, MemPoolType, query_result_to_history};
use core_history::history_store::HistoryStore;
use core_history::store::SlateDBHistoryStore;
use core_metastore::{Metastore, SlateDBMetastore, TableIdent as MetastoreTableIdent};
use core_utils::Db;
use df_catalog::catalog_list::EmbucketCatalogList;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait ExecutionService: Send + Sync {
    async fn create_session(&self, session_id: String) -> Result<Arc<UserSession>>;
    async fn delete_session(&self, session_id: String) -> Result<()>;
    async fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>>;
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
    concurrency_limit: Arc<Semaphore>,
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
        let max_concurrency_level = config.max_concurrency_level;
        let runtime_env = Self::runtime_env(&config, catalog_list.clone())?;
        Ok(Self {
            metastore,
            history_store,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            catalog_list,
            runtime_env,
            concurrency_limit: Arc::new(Semaphore::new(max_concurrency_level)),
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
        err
    )]
    async fn create_session(&self, session_id: String) -> Result<Arc<UserSession>> {
        {
            let sessions = self.df_sessions.read().await;
            if let Some(session) = sessions.get(&session_id) {
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
            sessions.insert(session_id.clone(), user_session.clone());
        }
        Ok(user_session)
    }

    #[tracing::instrument(
        name = "ExecutionService::delete_session",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_session(&self, session_id: String) -> Result<()> {
        // TODO: Need to have a timeout for the lock
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(&session_id);
        Ok(())
    }
    async fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>> {
        self.df_sessions.clone()
    }

    #[tracing::instrument(name = "ExecutionService::query", level = "debug", skip(self), err)]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryResult> {
        // Attempt to acquire a concurrency permit without waiting.
        // This immediately returns an error if the concurrency limit has been reached.
        // If you want the task to wait until a permit becomes available, use `.acquire().await` instead.

        // Holding this permit ensures that no more than the configured number of concurrent queries
        // can execute at the same time. When the permit is dropped, the slot is released back to the semaphore.
        let _permit = self
            .concurrency_limit
            .try_acquire()
            .context(ex_error::ConcurrencyLimitSnafu)?;

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

        let mut history_record = self
            .history_store
            .query_record(query, query_context.worksheet_id);
        // Attach the generated query ID to the query context before execution.
        // This ensures consistent tracking and logging of the query across all layers.
        let mut query_obj = user_session.query(
            query,
            query_context.with_query_id(history_record.query_id()),
        );

        // Execute the query with a timeout to prevent long-running or stuck queries
        // from blocking system resources indefinitely. If the timeout is exceeded,
        // convert the timeout into a standard QueryTimeout error so it can be handled
        // and recorded like any other execution failure.
        let result = timeout(
            Duration::from_secs(self.config.query_timeout_secs),
            query_obj.execute(),
        )
        .await;
        let query_result: Result<QueryResult> = match result {
            Ok(inner_result) => inner_result,
            Err(_) => Err(ex_error::QueryTimeoutSnafu.build()),
        };

        // Record the query in the session’s history, including result count or error message.
        // This ensures all queries are traceable and auditable within a session, which enables
        // features like `last_query_id()` and enhances debugging and observability.
        self.history_store
            .save_query_record(&mut history_record, query_result_to_history(&query_result))
            .await;
        query_result
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
