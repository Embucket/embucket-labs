use std::{collections::HashMap, env, sync::Arc};

use arrow::array::RecordBatch;
use arrow_json::{writer::JsonArray, WriterBuilder};
use bytes::Bytes;
use datafusion::{execution::SessionStateBuilder, prelude::{SessionConfig, SessionContext}, sql::planner::IdentNormalizer};
use datafusion_iceberg::planner::IcebergQueryPlanner;
use object_store::{path::Path, PutPayload};
use snafu::{OptionExt, ResultExt};
use uuid::Uuid;
use crate::execution::{datafusion::type_planner::IceBucketTypePlanner, session::IceBucketSessionParams};

use super::{models::ColumnInfo, query::IceBucketQuery, session::IceBucketUserSession, utils::{convert_record_batches, Config}};
use icebucket_metastore::{IceBucketTableIdent, Metastore};
use tokio::sync::RwLock;

use super::error::{self as ex_error, ExecutionError, ExecutionResult};

pub struct ExecutionService {
    metastore: Arc<dyn Metastore>,
    df_sessions: Arc<RwLock<HashMap<String, Arc<IceBucketUserSession>>>>,
    config: Arc<Config>,
}

impl ExecutionService {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        config: Arc<Config>,

    ) -> Self {
        Self {
            metastore,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_session(&self, session_id: String) -> ExecutionResult<()> {
        let session_exists = { self.df_sessions.read().await.contains_key(&session_id) };
        if !session_exists {
            let user_session = IceBucketUserSession::new()?;
            tracing::trace!("Acuiring write lock for df_sessions");
            let mut session_list_mut = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            session_list_mut.insert(session_id, Arc::new(user_session));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_session(&self, session_id: String) -> ExecutionResult<()> {
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
    ) -> ExecutionResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let sessions = self.df_sessions.read().await;
        let user_session =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;
        let query_obj = IceBucketQuery::new(
            self.metastore.clone(), 
            query.to_string(), 
            user_session.clone()
        );
        
        let records: Vec<RecordBatch> = query_obj.execute().await?;

        let data_format = self.config().data_format;
        // Add columns dbt metadata to each field
        convert_record_batches(records, data_format).context(ex_error::DataFusionQuerySnafu { query })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_table(&self, session_id: &str, query: &str) -> ExecutionResult<String> {
        let (records, _) = self.query(session_id, query).await?;
        let buf = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(buf);

        let record_refs: Vec<&RecordBatch> = records.iter().collect();
        writer
            .write_batches(&record_refs)
            .context(ex_error::ArrowSnafu)?;
        writer.finish().context(ex_error::ArrowSnafu)?;

        // Get the underlying buffer back,
        let buf = writer.into_inner();

        Ok(String::from_utf8(buf).context(ex_error::Utf8Snafu)?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &IceBucketTableIdent,
        data: Bytes,
        file_name: String,
    ) -> ExecutionResult<()> {
        let sessions = self.df_sessions.read().await;
        let user_session =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;
        let unique_file_id = Uuid::new_v4().to_string();
        let metastore_db = self.metastore.get_database(&table_ident.database).await
                .context(ex_error::MetastoreSnafu)?
                .ok_or(ExecutionError::DatabaseNotFound { db: table_ident.database.clone() })?;

        let object_store = self.metastore.volume_object_store(&metastore_db.volume).await
            .context(ex_error::MetastoreSnafu)?;

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_path = self.metastore.url_for_table(&table_ident).await
            .context(ex_error::MetastoreSnafu)?;
        let upload_path = format!("{table_path}/tmp/{unique_file_id}/{file_name}");

        let path = Path::from(upload_path.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(ex_error::ObjectStoreSnafu)?;

        user_session
            .ctx
            .register_object_store(&upload_path, Arc::from(object_store));
        user_session
            .ctx
            .register_csv(table_name, path_string, CsvReadOptions::new())
            .await?;

        let insert_query = format!(
            "INSERT INTO {warehouse_name}.{database_name}.{table_name} SELECT * FROM {table_name}"
        );
        executor
            .execute_with_custom_plan(&insert_query, warehouse_name.as_str())
            .await
            .context(error::ExecutionSnafu)?;

        
        Ok(())
    }

    fn config(&self) -> &Config {
        &self.config
    }
}

