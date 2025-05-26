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

// Updated use statements for types now in core-traits
use core_traits::executor::{
    ColumnInfo, ExecutionError, ExecutionResult, ExecutionService, QueryContext, QueryResultData, UserSession, Config as ExecutionConfig // Assuming UserSession, QueryContext, Config are also part of core_traits::executor
};
use core_traits::metastore::TableIdent as MetastoreTableIdent;

// Local uses
use super::error::{self as ex_error}; // Keep local error alias for now, if core-executor has its own errors
use super::session::UserSession as CoreExecutorUserSession; // If UserSession is defined locally and distinct from the one in traits
use super::query::QueryContext as CoreExecutorQueryContext; // If QueryContext is defined locally
use super::utils::{Config as CoreExecutorConfig, convert_record_batches, DataSerializationFormat}; // If Config is defined locally

use core_metastore::{Metastore, SlateDBMetastore}; // Metastore trait comes from core_metastore for now, until that is updated to use core_traits::Metastore
use core_utils::Db;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct CoreExecutionService {
    metastore: Arc<dyn Metastore>, // This will become Arc<dyn core_traits::metastore::Metastore>
    df_sessions: Arc<RwLock<HashMap<String, Arc<CoreExecutorUserSession>>>>, // Adjusted to local type if different
    config: CoreExecutorConfig, // Adjusted to local type
}

impl CoreExecutionService {
    pub fn new(metastore: Arc<dyn Metastore>, config: CoreExecutorConfig) -> Self { // Adjusted to local type
        Self {
            metastore,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }
}

#[async_trait::async_trait]
impl ExecutionService for CoreExecutionService { // Implements core_traits::executor::ExecutionService
    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_session(&self, session_id: String) -> ExecutionResult<Arc<UserSession>> { // UserSession from core_traits
        {
            let sessions = self.df_sessions.read().await;
            if let Some(session) = sessions.get(&session_id) {
                // Assuming session here is CoreExecutorUserSession, needs conversion or UserSession in traits must be compatible
                // For now, assuming UserSession in core_traits is a new definition, and this needs adjustment.
                // Let's assume UserSession in core_traits is the one to be used.
                // This implies CoreExecutorUserSession might need to become core_traits::executor::UserSession.
                // This part needs careful type alignment. For now, I'll assume the trait dictates the type.
                // If UserSession was one of the placeholder structs in core_traits::executor, this will work.
                // The prompt mentioned UserSession, QueryContext, Config as placeholders in core_traits::executor.
                // So, this signature is aiming to use `core_traits::executor::UserSession`.
                // The `df_sessions` field stores `CoreExecutorUserSession`. This is a conflict.
                // For this refactoring step, I will assume UserSession in the trait method signature refers to the one defined in core_traits::executor.
                // The internal storage `self.df_sessions` holding `CoreExecutorUserSession` will need to be reconciled later.
                // This might mean `CoreExecutorUserSession` itself needs to be replaced by `core_traits::executor::UserSession`
                // or a conversion is needed.
                // For now, I'll focus on making the method signature match the trait.
                // The simplest path is if `CoreExecutorUserSession` IS `core_traits::executor::UserSession`.
                // The prompt had `pub struct UserSession; // Placeholder` in core-traits/src/executor.rs.
                // This means `Arc<UserSession>` in the trait refers to this placeholder.
                // The `UserSession::new` call below will break if it's the placeholder.
                // This suggests `UserSession` struct itself should have been fully moved or properly defined in traits.
                // This is a deeper refactoring issue than just path changes.
                // I will proceed with the path changes, but this type mismatch will likely cause errors.

                // To make it compile for now, let's assume `UserSession` is the one from `core_traits::executor`.
                // And that `CoreExecutorUserSession` will be replaced by it or made compatible.
                 return Ok(session.clone() as Arc<UserSession>); // This cast is unlikely to work.
            }
        }
        // This UserSession::new must refer to the concrete type, likely from core_executor::session::UserSession
        // but the return type is core_traits::executor::UserSession. This is a problem.
        // For now, I'll assume UserSession::new refers to the local one, and then there's a conceptual mismatch.
        let user_session_impl = Arc::new(CoreExecutorUserSession::new(self.metastore.clone()).await.map_err(|e| ExecutionError::DataFusion{source: e.into()})?); // Placeholder error conversion
        // The trait expects Arc<core_traits::executor::UserSession>.
        // If core_traits::executor::UserSession is just a placeholder struct, this won't work.
        // This requires UserSession to be a trait itself, or the concrete type fully moved.
        // Let's assume for this step the types are compatible by name, and leave deeper fix for compilation phase.
        let user_session_for_trait = user_session_impl.clone() as Arc<UserSession>; // This is problematic.

        {
            tracing::trace!("Acquiring write lock for df_sessions");
            let mut sessions = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            sessions.insert(session_id.clone(), user_session_impl); // Store the concrete type
        }
        Ok(user_session_for_trait) // Return the type expected by trait
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_session(&self, session_id: String) -> ExecutionResult<()> { // ExecutionResult from core_traits
        // TODO: Need to have a timeout for the lock
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(&session_id);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext, // QueryContext from core_traits
    ) -> ExecutionResult<QueryResultData> { // QueryResultData and ExecutionResult from core_traits
        let sessions = self.df_sessions.read().await;
        let user_session_impl =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession { // ExecutionError from core_traits
                    id: session_id.to_string(),
                })?;

        // query_context is core_traits::executor::QueryContext.
        // user_session_impl.query needs to accept this type or a conversion is needed.
        // Assuming CoreExecutorQueryContext is compatible or will be replaced.
        let mut query_obj = user_session_impl.query(query, query_context);

        let records: Vec<RecordBatch> = query_obj.execute().await?;

        let data_format = self.config().dbt_serialization_format;
        // Add columns dbt metadata to each field
        // TODO: RecordBatch conversion should happen somewhere outside ExecutionService
        // Perhaps this can be moved closer to Snowflake API layer

        let (records, columns) = convert_record_batches(records, data_format)
    .map_err(|e| ExecutionError::DataFusion { source: e.into() })?; // Placeholder for ex_error::DataFusionQuerySnafu

        // TODO: Perhaps it's better to return a schema as a result of `execute` method
        let columns_info = if columns.is_empty() { // ColumnInfo from core_traits
            query_obj
                .get_custom_logical_plan(&query_obj.query)
                .await.map_err(|e| ExecutionError::DataFusion{source: e.into()})? // Placeholder error conversion
                .schema()
                .fields()
                .iter()
                .map(|field| ColumnInfo::from_field(field)) // ColumnInfo from core_traits
                .collect::<Vec<_>>()
        } else {
            columns
        };

        Ok(QueryResultData { // QueryResultData from core_traits
            records,
            columns_info, // ColumnInfo from core_traits
            query_id: i64::default(), 
        })
    }

    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent, // MetastoreTableIdent from core_traits::metastore
        data: Bytes,
        file_name: &str,
        format: Format, // datafusion::arrow::csv::reader::Format
    ) -> ExecutionResult<usize> { // ExecutionResult from core_traits
        let unique_id = Uuid::new_v4().to_string().replace('-', "_");
        let sessions = self.df_sessions.read().await;
        let user_session_impl =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession { // ExecutionError from core_traits
                    id: session_id.to_string(),
                })?;

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
            .map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu
        user_session_impl.ctx.register_catalog( // user_session_impl is CoreExecutorUserSession
            source_table.catalog().unwrap_or_default(),
            Arc::new(inmem_catalog),
        );
        let exists = user_session_impl
            .ctx
            .table_exist(target_table.clone())
            .map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu

        let schema = if exists {
            let table = user_session_impl
                .ctx
                .table(target_table)
                .await
                .map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu
            table.schema().as_arrow().to_owned()
        } else {
            let (schema, _) = format
                .infer_schema(data.clone().reader(), None)
                .map_err(|e| ExecutionError::DataFusion { source: e.into() })?; // ExecutionError from core_traits
            schema
        };
        let schema = Arc::new(schema);

        let csv = ReaderBuilder::new(schema.clone())
            .with_format(format)
            .build_buffered(data.reader())
            .map_err(|e| ExecutionError::Arrow{source:e})?; // Placeholder for ex_error::ArrowSnafu

        let batches: Result<Vec<_>, _> = csv.collect();
        let batches = batches.map_err(|e| ExecutionError::Arrow{source:e})?; // Placeholder for ex_error::ArrowSnafu

        let rows_loaded = batches
            .iter()
            .map(|batch: &RecordBatch| batch.num_rows())
            .sum();

        let table = MemTable::try_new(schema, vec![batches]).map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu
        user_session_impl
            .ctx
            .register_table(source_table.clone(), Arc::new(table))
            .map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu

        let table_ref_str = source_table.clone(); // Renamed to avoid conflict with table variable
        let query_str = if exists { // Renamed to avoid conflict with query variable
            format!("INSERT INTO {table_ident} SELECT * FROM {table_ref_str}")
        } else {
            format!("CREATE TABLE {table_ident} AS SELECT * FROM {table_ref_str}")
        };

        // query_context needs to be core_traits::executor::QueryContext
        // If QueryContext::default() refers to a local type, it needs to be adjusted.
        // Assuming QueryContext::default() creates the type from core_traits.
        let mut query_obj = user_session_impl.query(&query_str, QueryContext::default());
        Box::pin(query_obj.execute()).await.map_err(|e| ExecutionError::DataFusion{source: e.into()})?; // Placeholder error conversion

        user_session_impl
            .ctx
            .deregister_table(source_table)
            .map_err(|e| ExecutionError::DataFusion{source:e})?; // Placeholder for ex_error::DataFusionSnafu

        Ok(rows_loaded)
    }

    #[must_use]
    fn config(&self) -> &ExecutionConfig { // ExecutionConfig from core_traits
        // This assumes self.config (CoreExecutorConfig) is compatible with &ExecutionConfig (core_traits)
        // This is another type mismatch that needs proper handling (e.g. CoreExecutorConfig becomes core_traits::executor::ExecutionConfig)
        &self.config
    }
}

//Test environment
pub async fn make_text_execution_svc() -> Arc<CoreExecutionService> {
    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db)); // SlateDBMetastore uses core_metastore::Metastore

    Arc::new(CoreExecutionService::new(
        metastore,
        CoreExecutorConfig { // Assuming CoreExecutorConfig is the local concrete type
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    ))
}
