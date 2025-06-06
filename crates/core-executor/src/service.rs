use std::vec;
use std::{collections::HashMap, sync::Arc};

use bytes::{Buf, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::csv::reader::Format;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::memory::MemTable;
use datafusion_common::TableReference;
use snafu::ResultExt;

use super::error::{self as ex_error, ExecutionError, ExecutionResult};
use super::{models::QueryContext, models::QueryResult, session::UserSession};
use crate::utils::{Config, query_result_to_history};
use core_history::history_store::HistoryStore;
use core_history::store::SlateDBHistoryStore;
use core_metastore::{Metastore, SlateDBMetastore, TableIdent as MetastoreTableIdent};
use core_utils::Db;
use tokio::sync::RwLock;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait ExecutionService: Send + Sync {
    async fn create_session(&self, session_id: String) -> ExecutionResult<Arc<UserSession>>;
    async fn delete_session(&self, session_id: String) -> ExecutionResult<()>;
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> ExecutionResult<QueryResult>;
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> ExecutionResult<usize>;
}

pub struct CoreExecutionService {
    metastore: Arc<dyn Metastore>,
    history_store: Arc<dyn HistoryStore>,
    df_sessions: Arc<RwLock<HashMap<String, Arc<UserSession>>>>,
    config: Arc<Config>,
}

impl CoreExecutionService {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        history_store: Arc<dyn HistoryStore>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            metastore,
            history_store,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
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
    async fn create_session(&self, session_id: String) -> ExecutionResult<Arc<UserSession>> {
        {
            let sessions = self.df_sessions.read().await;
            if let Some(session) = sessions.get(&session_id) {
                return Ok(session.clone());
            }
        }
        let user_session = Arc::new(
            UserSession::new(
                self.metastore.clone(),
                self.history_store.clone(),
                self.config.clone(),
            )
            .await?,
        );
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
    async fn delete_session(&self, session_id: String) -> ExecutionResult<()> {
        // TODO: Need to have a timeout for the lock
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(&session_id);
        Ok(())
    }

    #[tracing::instrument(name = "ExecutionService::query", level = "debug", skip(self), err)]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> ExecutionResult<QueryResult> {
        let user_session = {
            let sessions = self.df_sessions.read().await;
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
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

        let query_result = query_obj.execute().await;
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
    ) -> ExecutionResult<usize> {
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
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
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
                .map_err(|e| ExecutionError::DataFusion { source: e.into() })?;
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

        let batches: Result<Vec<_>, _> = csv.collect();
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
pub async fn make_text_execution_svc() -> Arc<CoreExecutionService> {
    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    Arc::new(CoreExecutionService::new(
        metastore,
        history_store,
        Arc::new(Config::default()),
    ))
}
