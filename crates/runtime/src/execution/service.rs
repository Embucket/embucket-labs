use std::{collections::HashMap, env, sync::Arc};

use arrow::array::RecordBatch;
use arrow_json::{writer::JsonArray, WriterBuilder};
use bytes::Bytes;
use datafusion::{execution::SessionStateBuilder, prelude::{SessionConfig, SessionContext}, sql::planner::IdentNormalizer};
use datafusion_iceberg::planner::IcebergQueryPlanner;
use snafu::ResultExt;
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
        let warehouse = self.get_warehouse(*warehouse_id).await?;
        let warehouse_name = warehouse.name.clone();
        let storage_profile = self.get_profile(warehouse.storage_profile_id).await?;
        let object_store = storage_profile
            .get_object_store()
            .context(error::InvalidStorageProfileSnafu)?;
        let unique_file_id = Uuid::new_v4().to_string();

        let sessions = self.df_sessions.read().await;
        let executor =
            sessions
                .get(session_id)
                .ok_or(ControlPlaneError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_part = format!("{}/{}", warehouse.path(), table_name);
        let path_string = format!("{table_part}/tmp/{unique_file_id}/{file_name}");

        let path = Path::from(path_string.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(error::ObjectStoreSnafu)?;

        if executor.ctx.catalog(warehouse.name.as_str()).is_none() {
            // Create table from CSV
            let config = {
                let mut config = Configuration::new();
                config.base_path = "http://0.0.0.0:3000/catalog".to_string();
                config
            };
            let object_store_builder = storage_profile
                .get_object_store_builder()
                .context(error::InvalidStorageProfileSnafu)?;
            let rest_client = RestCatalog::new(
                Some(warehouse_id.to_string().as_str()),
                config,
                object_store_builder,
            );
            let catalog = IcebergCatalog::new(Arc::new(rest_client), None).await?;
            executor
                .ctx
                .register_catalog(warehouse.name.clone(), Arc::new(catalog));
        }

        let endpoint_url = storage_profile
            .get_object_store_endpoint_url()
            .map_err(|_| ControlPlaneError::MissingStorageEndpointURL)?;

        let path_string = if let Some(creds) = &storage_profile.credentials {
            match &creds {
                Credentials::AccessKey(_) => {
                    // If the storage profile is AWS S3, modify the path_string with the S3 prefix
                    format!("{endpoint_url}/{path_string}")
                }
                Credentials::Role(_) => path_string,
            }
        } else {
            format!("{endpoint_url}/{path_string}")
        };
        executor
            .ctx
            .register_object_store(&endpoint_url, Arc::from(object_store));
        executor
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

        // let config = {
        //     let mut config = Configuration::new();
        //     config.base_path = "http://0.0.0.0:3000/catalog".to_string();
        //     config
        // };
        // let rest_client = RestCatalog::new(
        //     Some(warehouse_id.to_string().as_str()),
        //     config,
        //     storage_profile.get_object_store_builder(),
        // );
        // let catalog = IcebergCatalog::new(Arc::new(rest_client), None).await?;
        // let ctx = SessionContext::new();
        // let catalog_name = warehouse.name.clone();
        // ctx.register_catalog(catalog_name.clone(), Arc::new(catalog));
        //
        // // register a local file system object store for /tmp directory
        // // create partitioned input file and context
        // let local = Arc::new(LocalFileSystem::new_with_prefix("").unwrap());
        // let local_url = Url::parse("file://").unwrap();
        // ctx.register_object_store(&local_url, local);
        //
        // let provider = ctx.catalog(catalog_name.clone().as_str()).unwrap();
        // let table = provider.schema(database_name).unwrap().table(table_name).await?;
        // let table_schema = table.unwrap().schema();
        // let df = ctx
        //     .read_csv(path_string.clone(), CsvReadOptions::new().schema(&*table_schema))
        //     .await?;
        // let data = df.collect().await?;

        //
        // let input = ctx.read_batches(data)?;
        // input.write_table(format!("{catalog_name}.{database_name}.{table_name}").as_str(),
        //                   DataFrameWriteOptions::default()).await?;
        // Ok(())

        //////////////////////////////////////

        // let config = {
        //     HashMap::from([
        //         ("iceberg.catalog.type".to_string(), "rest".to_string()),
        //         (
        //             "iceberg.catalog.demo.warehouse".to_string(),
        //             warehouse_id.to_string(),
        //         ),
        //         ("iceberg.catalog.name".to_string(), "demo".to_string()),
        //         (
        //             "iceberg.catalog.demo.uri".to_string(),
        //             "http://0.0.0.0:3000/catalog".to_string(),
        //         ),
        //         (
        //             "iceberg.table.io.region".to_string(),
        //             storage_profile.region.to_string(),
        //         ),
        //         (
        //             "iceberg.table.io.endpoint".to_string(),
        //             storage_endpoint_url.to_string(),
        //         ),
        //         // (
        //         //     "iceberg.table.io.bucket".to_string(),
        //         //     "examples".to_string(),
        //         // ),
        //         // (
        //         //     "iceberg.table.io.access_key_id".to_string(),
        //         //     "minioadmin".to_string(),
        //         // ),
        //         // (
        //         //     "iceberg.table.io.secret_access_key".to_string(),
        //         //     "minioadmin".to_string(),
        //         // ),
        //     ])
        // };
        // let catalog = icelake::catalog::load_catalog(&config).await?;
        // let table_ident = TableIdentifier::new(vec![database_name, table_name])?;
        // let mut table = catalog.load_table(&table_ident).await?;
        // let table_schema = table.current_arrow_schema()?;
        // println!("{:?}", table.table_name());
        //
        // let df = ctx
        //     .read_csv(path_string, CsvReadOptions::new().schema(&table_schema))
        //     .await?;
        // let data = df.collect().await?;
        //
        // let builder = table.writer_builder()?.rolling_writer_builder(None)?;
        // let mut writer = table
        //     .writer_builder()?
        //     .build_append_only_writer(builder)
        //     .await?;
        //
        // for r in data {
        //     writer.write(&r).await?;
        // }
        //
        // let res: Vec<icelake::types::DataFile> = writer.close().await?;
        // let mut txn = icelake::transaction::Transaction::new(&mut table);
        // txn.append_data_file(res);
        // txn.commit().await?;

        Ok(())
    }

    fn config(&self) -> &Config {
        &self.config
    }
}

