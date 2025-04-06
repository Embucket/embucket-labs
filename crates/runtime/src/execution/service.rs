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
use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::CatalogProvider;
use datafusion::datasource::memory::MemTable;
use datafusion_common::TableReference;
use snafu::ResultExt;

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
    df_sessions: Arc<RwLock<HashMap<String, Arc<IceBucketUserSession>>>>,
    config: Config,
}

impl ExecutionService {
    pub fn new(metastore: Arc<dyn Metastore>, config: Config) -> Self {
        Self {
            metastore,
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
    pub async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &IceBucketTableIdent,
        _data: Bytes,
        file_name: &str,
    ) -> ExecutionResult<()> {
        let sessions = self.df_sessions.read().await;
        let user_session =
            sessions
                .get(session_id)
                .ok_or(ExecutionError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;

        let src_table_ref = "tmp_db.tmp_schema.tmp_table";
        let inmem_catalog = MemoryCatalogProvider::new();
        inmem_catalog
            .register_schema("tmp_schema", Arc::new(MemorySchemaProvider::new()))
            .context(ex_error::DataFusionSnafu)?;
        user_session
            .ctx
            .register_catalog("tmp_db", Arc::new(inmem_catalog));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let value_array = Float64Array::from(vec![10.5, 20.7, 30.2]);
        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Create the record batch
        #[allow(clippy::expect_used)]
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .expect("Failed to create record batch");

        #[allow(clippy::expect_used)]
        let table =
            MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("Failed to create table");
        user_session
            .ctx
            .register_table(
                TableReference::full("tmp_db", "tmp_schema", "tmp_table"),
                Arc::new(table),
            )
            .context(ex_error::DataFusionSnafu)?;

        // If target table already exists, we need to insert into it
        // otherwise, we need to create it
        let exists = user_session
            .ctx
            .table_exist(TableReference::full(
                table_ident.database.clone(),
                table_ident.schema.clone(),
                table_ident.table.clone(),
            ))
            .context(ex_error::DataFusionSnafu)?;

        let query = if exists {
            format!("INSERT INTO {table_ident} SELECT * FROM {src_table_ref}")
        } else {
            format!("CREATE TABLE {table_ident} AS SELECT * FROM {src_table_ref}")
        };

        let query = user_session.query(&query, IceBucketQueryContext::default());
        query.execute().await?;

        user_session
            .ctx
            .deregister_table(TableReference::full("tmp_db", "tmp_schema", "tmp_table"))
            .context(ex_error::DataFusionSnafu)?;

        Ok(())
    }

    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }
}
