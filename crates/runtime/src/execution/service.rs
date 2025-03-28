// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_json::{writer::JsonArray, WriterBuilder};
use bytes::Bytes;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::CsvReadOptions};
use icebucket_history::Worksheet;
use icebucket_history::{store::WorksheetsStore, QueryRecord, QueryRecordId};
use object_store::{path::Path, PutPayload};
use snafu::ResultExt;
use uuid::Uuid;

use super::{
    models::ColumnInfo,
    query::IceBucketQueryContext,
    session::IceBucketUserSession,
    utils::{convert_record_batches, Config},
};
use icebucket_metastore::{IceBucketTableIdent, Metastore};
use tokio::sync::RwLock;

use super::error::{self as ex_error, ExecutionError, ExecutionResult};

pub struct ExecutionService {
    metastore: Arc<dyn Metastore>,
    // TODO: Execution service shouldn't be responsible for history
    history: Arc<dyn WorksheetsStore>,
    df_sessions: Arc<RwLock<HashMap<String, Arc<IceBucketUserSession>>>>,
    config: Config,
}

impl ExecutionService {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        history: Arc<dyn WorksheetsStore>,
        config: Config,
    ) -> Self {
        Self {
            metastore,
            history,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_session(&self, session_id: String) -> ExecutionResult<()> {
        let session_exists = { self.df_sessions.read().await.contains_key(&session_id) };
        if !session_exists {
            let user_session = IceBucketUserSession::new(self.metastore.clone()).await?;
            tracing::trace!("Acuiring write lock for df_sessions");
            let mut session_list_mut = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            session_list_mut.insert(session_id, Arc::new(user_session));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn delete_session(&self, session_id: String) -> ExecutionResult<()> {
        // TODO: Need to have a timeout for the lock
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(&session_id);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[allow(clippy::large_futures)]
    pub async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: IceBucketQueryContext,
    ) -> ExecutionResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let sessions = self.df_sessions.read().await;
        let user_session =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;

        let query_obj = user_session.query(query, query_context);

        let records: Vec<RecordBatch> = query_obj.execute().await?;

        let data_format = self.config().dbt_serialization_format;
        // Add columns dbt metadata to each field
        // TODO: RecordBatch conversion should happen somewhere outside ExecutionService
        // Perhaps this can be moved closer to Snowflake API layer
        let (records, columns) = convert_record_batches(records, data_format)
            .context(ex_error::DataFusionQuerySnafu { query })?;

        // TODO: Perhaps it's better to return a schema as a result of `execute` method
        let columns = if columns.is_empty() {
            query_obj
                .plan()
                .await
                .map_err(|e| ExecutionError::DataFusionQuery {
                    query: query.to_string(),
                    source: e,
                })?
                .schema()
                .fields()
                .iter()
                .map(|field| ColumnInfo::from_field(field))
                .collect::<Vec<_>>()
        } else {
            columns
        };

        Ok((records, columns))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn query_table(
        &self,
        session_id: &str,
        worksheet: Worksheet,
        query: &str,
        query_context: IceBucketQueryContext,
    ) -> ExecutionResult<(QueryRecordId, String)> {
        let mut query_record = QueryRecord::query_start(worksheet.id, query, None);
        let id: QueryRecordId = query_record.id;

        // let (records, _) = self.query(session_id, query, query_context).await?;
        let records_batch = self.query(session_id, query, query_context).await;
        let res = match records_batch {
            Ok((records, _)) => {
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

                let res = String::from_utf8(buf)
                    .map(|res| (id, res))
                    .context(ex_error::Utf8Snafu);

                match &res {
                    Ok(id_res_tuple) => {
                        let result_count = i64::try_from(records.len()).unwrap_or(0);
                        query_record.query_finished(
                            result_count,
                            Some(id_res_tuple.1.clone()),
                            None,
                        );
                    }
                    Err(err) => {
                        query_record.query_finished_with_error(err.to_string());
                    }
                };

                Ok(res)
            }
            Err(err) => {
                query_record.query_finished_with_error(err.to_string());

                Err(err)
            }
        };

        if let Err(err) = self.history.add_history_item(query_record.clone()).await {
            // do not raise error, just log ?
            tracing::error!("{err}");
        }

        res?
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn upload_data_to_table(
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
        let metastore_db = self
            .metastore
            .get_database(&table_ident.database)
            .await
            .context(ex_error::MetastoreSnafu)?
            .ok_or(ExecutionError::DatabaseNotFound {
                db: table_ident.database.clone(),
            })?;

        let object_store = self
            .metastore
            .volume_object_store(&metastore_db.volume)
            .await
            .context(ex_error::MetastoreSnafu)?
            .ok_or(ExecutionError::VolumeNotFound {
                volume: metastore_db.volume.clone(),
            })?;

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_path = self
            .metastore
            .url_for_table(table_ident)
            .await
            .context(ex_error::MetastoreSnafu)?;
        let upload_path = format!("{table_path}/tmp/{}/{file_name}", unique_file_id.clone());

        let path = Path::from(upload_path.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(ex_error::ObjectStoreSnafu)?;

        let temp_table_ident = IceBucketTableIdent {
            database: "datafusion".to_string(),
            schema: "tmp".to_string(),
            table: unique_file_id.clone(),
        };

        // We construct this URL so we can unwrap it
        #[allow(clippy::unwrap_used)]
        user_session.ctx.register_object_store(
            ObjectStoreUrl::parse(&upload_path).unwrap().as_ref(),
            object_store.clone(),
        );
        user_session
            .ctx
            .register_csv(
                temp_table_ident.to_string(),
                upload_path,
                CsvReadOptions::new(),
            )
            .await
            .context(ex_error::DataFusionSnafu)?;

        let insert_query = format!("INSERT INTO {table_ident} SELECT * FROM {temp_table_ident}",);

        let query = user_session.query(&insert_query, IceBucketQueryContext::default());

        query.execute().await?;

        user_session
            .ctx
            .deregister_table(temp_table_ident.to_string())
            .context(ex_error::DataFusionSnafu)?;

        object_store
            .delete(&path)
            .await
            .context(ex_error::ObjectStoreSnafu)?;
        Ok(())
    }

    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }
}
