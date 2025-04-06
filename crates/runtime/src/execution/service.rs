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
use bytes::Bytes;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::CsvReadOptions};
use datafusion_common::{TableReference};
use object_store::{path::Path, PutPayload};
use snafu::ResultExt;
use uuid::Uuid;

use super::{
    models::ColumnInfo,
    query::IceBucketQueryContext,
    session::IceBucketUserSession,
    utils::{convert_record_batches, Config},
};
use icebucket_metastore::{IceBucketTableIdent, Metastore, IceBucketVolumeType};
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

    #[tracing::instrument(level = "debug", skip(self, data))]
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

        let metastore_volume = self.metastore
            .get_volume(&metastore_db.volume)
            .await
            .context(ex_error::MetastoreSnafu)?
            .ok_or(ExecutionError::VolumeNotFound {
                volume: metastore_db.volume.clone(),
            })?;

        // construct URL, so it can be used to put csv file, which can be registered as a table

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        // let table_path = self
        //     .metastore
        //     .url_for_table(table_ident)
        //     .await
        //     .context(ex_error::MetastoreSnafu)?;

        let volume_path = if let IceBucketVolumeType::File(ref file_volume)  = metastore_volume.volume {
            file_volume.path.clone()
        }
        else {
            String::new()
        };

        let IceBucketTableIdent { database, schema, table } = table_ident;
        // let table_path = format!("{database}_{schema}_{table}_csv_{unique_file_id}_{file_name}");
        let table_path = format!("{volume_path}/{database}_{schema}_{table}_{unique_file_id}_{file_name}");
        let data_location = Path::from_absolute_path(table_path.clone()).unwrap();
        let table_path = data_location.as_ref().to_string(); // get rid of leading '/'

        // metastore_volume.prefix(), table_ident.database, table_ident.schema, table_ident.table
        // let object_url = format!("{table_path}/csv/{}/{file_name}", unique_file_id.clone());
        // let object_store_url = metastore_volume.prefix();
        let object_store_url = "file://".to_string();
        let upload_uri = format!("{object_store_url}/{table_path}");

        let object_store = self
            .metastore
            .volume_object_store(&metastore_db.volume)
            .await
            .context(ex_error::MetastoreSnafu)?
            .ok_or(ExecutionError::VolumeNotFound {
                volume: metastore_db.volume.clone(),
            })?;

        
        println!("table_path: {table_path}, \ndata_location: {data_location}, \nobject_store_url: {object_store_url}, \nupload_uri: {upload_uri}");

        object_store
            .put(&data_location, PutPayload::from_bytes(data))
            .await
            .context(ex_error::ObjectStoreSnafu)?;

        println!("register_object_store");
        // We construct this URL so we can unwrap it
        #[allow(clippy::unwrap_used)]
        user_session.ctx.register_object_store(
            ObjectStoreUrl::parse(&object_store_url).unwrap().as_ref(),
            object_store.clone(),
        );

        println!("read_csv");
        let df = user_session
            .ctx
            .read_csv(upload_uri, CsvReadOptions::new())
            .await
            .context(ex_error::DataFusionSnafu)?;
        
        let schema = df.schema().clone();
        println!("Inferred Schema: {:?}", schema);

        let results = df.collect().await.context(ex_error::DataFusionSnafu)?;

        let arrow_schema = arrow_schema::Schema::from(schema);

    
        let temp_table_ident = IceBucketTableIdent {
            database: "icebucket".to_string(),
            schema: table_ident.schema.clone(),
            table: unique_file_id.clone(),
        };

        use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
        use datafusion_catalog::CatalogProvider;
        use crate::execution::catalog::IceBucketDFSchema;
        use datafusion::physical_plan::display::DisplayableExecutionPlan;
        use icebucket_metastore::{
            IceBucketSchema, IceBucketSchemaIdent, IceBucketTableCreateRequest, IceBucketTableFormat,
            IceBucketTableIdent, Metastore,
        };
        use datafusion_expr::logical_plan::dml::InsertOp;

        // let catalog = datafusion_catalog::MemoryCatalogProvider::new();
        // let schema_res = catalog.register_schema(
        //     temp_table_ident.schema.as_str(), 
        //     Arc::new(IceBucketDFSchema {
        //         database: temp_table_ident.database.clone(),
        //         schema: temp_table_ident.schema.clone(),
        //         metastore: self.metastore.clone(),
        //         mirror: Arc::new(dashmap::DashMap::new()),
        //     })
        // );

        // user_session.ctx.register_catalog(
        //     "icebucket", 
        //     Arc::new(catalog)
        // );
        // user_session.ctx.refresh_catalogs().await.context(ex_error::DataFusionSnafu)?;

        ////////////////////////// create target table
        use iceberg_rust_spec::types::StructType;
        use iceberg_rust::spec::arrow::schema::new_fields_with_ids;
        use datafusion_common::DataFusionError;
        use iceberg_rust::spec::schema::Schema;

        let fields_with_ids =
        StructType::try_from(&new_fields_with_ids(arrow_schema.fields(), &mut 0))
            .map_err(|err| DataFusionError::External(Box::new(err)))
            .context(ex_error::DataFusionSnafu)?;
        
        let table_create_request = IceBucketTableCreateRequest {
            ident: table_ident.clone(),
            schema: Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(fields_with_ids)
                .build()
                .map_err(|err| DataFusionError::External(Box::new(err)))
                .context(ex_error::DataFusionSnafu)?,
            location: None,
            partition_spec: None,
            sort_order: None,
            stage_create: None,
            volume_ident: None,
            is_temporary: Some(false),
            format: None,
            properties: None,
        };

        println!("Create table Schema: {:?}", table_create_request.schema);

        self.metastore
            .create_table(&table_ident, table_create_request)
            .await
            .context(ex_error::MetastoreSnafu)?;

        ////////////////////////
   

        // Create a Memory Table from the RecordBatch
        let mem_table = datafusion::datasource::MemTable::try_new(
            arrow_schema.clone().into(), vec![results]
        ).context(ex_error::DataFusionSnafu)?;
    
        // let tbl_proviser = user_session.ctx
        //     .register_table(temp_table_ident.to_string(), Arc::new(mem_table))
        //     .context(ex_error::DataFusionSnafu);


        let temp_table_name = temp_table_ident.table;
        // Use the provider_as_source function to convert the TableProvider to a table source
        let table_source = datafusion::datasource::provider_as_source(Arc::new(mem_table));

        // create a LogicalPlanBuilder for a table scan without projection or filters
        let scan_plan = LogicalPlanBuilder::scan(temp_table_name, table_source, None)
            .context(ex_error::DataFusionSnafu)?
            .build()
            .context(ex_error::DataFusionSnafu)?;

        // INSERT INTO MyTable VALUES (1), (2)
        let insert_plan = LogicalPlanBuilder::insert_into(
            scan_plan,
            table_ident.to_string(),
            &arrow_schema.clone(),
            InsertOp::Append,
        )
        .context(ex_error::DataFusionSnafu)?
        .build()
        .context(ex_error::DataFusionSnafu)?;

        // let foo = LogicalPlanBuilder::from(scan_plan);

        // let insert_plan = LogicalPlan::Insert {
        //     table_name: table_ident.to_string(),
        //     source: Box::new(logical_plan),
        //     overwrite: false,
        // };

        let physical_plan = user_session.ctx.state()
            .create_physical_plan(&insert_plan)
            .await
            .context(ex_error::DataFusionSnafu)?;

        // print the plan
        println!("physical_plan {}", DisplayableExecutionPlan::new(physical_plan.as_ref()).indent(true));

        let res = user_session.ctx.execute_logical_plan(insert_plan).await.context(ex_error::DataFusionSnafu)?;
        println!("res: {res:?}");
        
        // println!("register_csv");
        // user_session
        //     .ctx
        //     .register_csv(
        //         // table_ident.to_string(),
        //         temp_table_ident.to_string(),
        //         upload_uri,
        //         CsvReadOptions::new(),
        //     )
        //     .await
        //     .context(ex_error::DataFusionSnafu)?;



        // for catalog in user_session.ctx.state().catalog_list().catalog_names() {
        //     let provider = user_session
        //         .ctx
        //         .state()
        //         .catalog_list()
        //         .catalog(&catalog)
        //         .unwrap();
        //     println!("catalog: {catalog}");
        //     for schema in provider.schema_names() {
        //         println!("schema: {schema}");
        //         for table in provider.schema(&schema).unwrap().table_names() {
        //             let table_source = provider
        //                 .schema(&schema)
        //                 .unwrap()
        //                 .table(&table)
        //                 .await
        //                 .context(super::error::DataFusionSnafu)?
        //                 .ok_or(ExecutionError::TableProviderNotFound {
        //                     table_name: table.clone(),
        //                 })?;
        //             let resolved = user_session.ctx.state().resolve_table_ref(TableReference::full(
        //                 catalog.to_string(),
        //                 schema.to_string(),
        //                 table,
        //             ));
        //             println!("resolved: {resolved:?}");
        //         }
        //     }
        // }


        // let create_as_select_query = format!("CREATE TABLE {table_ident} AS SELECT * FROM '{}'", temp_table_ident.table);
        // let create_as_select_query = format!("CREATE TABLE {table_ident} AS SELECT * FROM memory_table");

        // println!("run query: {create_as_select_query}");

        // let query = user_session.query(&create_as_select_query, IceBucketQueryContext::default());

        // let insert_query = format!("INSERT INTO {table_ident} SELECT * FROM {temp_table_ident}",);
        //let query = user_session.query(&insert_query, IceBucketQueryContext::default());

        // println!("query.execute");

        // query.execute().await?;

        // println!("deregister");

        // user_session
        //     .ctx
        //     .deregister_table(temp_table_ident.to_string())
        //     .context(ex_error::DataFusionSnafu)?;

        println!("delete");

        object_store
            .delete(&data_location)
            .await
            .context(ex_error::ObjectStoreSnafu)?;
        Ok(())
    }

    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }
}
