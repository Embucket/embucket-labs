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

use crate::error::{self, ControlPlaneError, ControlPlaneResult};
use crate::models::{ColumnInfo, Credentials, StorageProfile, StorageProfileCreateRequest};
use crate::models::{Warehouse, WarehouseCreateRequest};
use crate::repository::{StorageProfileRepository, WarehouseRepository};
use crate::utils::convert_record_batches;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::JsonArray;
use arrow_json::WriterBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion_iceberg::planner::IcebergQueryPlanner;
use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use runtime::datafusion::execution::SqlExecutor;
use runtime::datafusion::session::SessionParams;
use runtime::datafusion::type_planner::CustomTypePlanner;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetBucketAclRequest, S3Client, S3};
use snafu::ResultExt;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[async_trait]
pub trait ControlService: Send + Sync {
    async fn create_profile(
        &self,
        params: &StorageProfileCreateRequest,
    ) -> ControlPlaneResult<StorageProfile>;
    async fn get_profile(&self, id: Uuid) -> ControlPlaneResult<StorageProfile>;
    async fn delete_profile(&self, id: Uuid) -> ControlPlaneResult<()>;
    async fn list_profiles(&self) -> ControlPlaneResult<Vec<StorageProfile>>;
    async fn validate_credentials(&self, profile: &StorageProfile) -> ControlPlaneResult<()>;

    async fn create_warehouse(
        &self,
        params: &WarehouseCreateRequest,
    ) -> ControlPlaneResult<Warehouse>;
    async fn get_warehouse_by_name(&self, name: String) -> ControlPlaneResult<Warehouse>;
    async fn get_warehouse(&self, id: Uuid) -> ControlPlaneResult<Warehouse>;
    async fn delete_warehouse(&self, id: Uuid) -> ControlPlaneResult<()>;
    async fn list_warehouses(&self) -> ControlPlaneResult<Vec<Warehouse>>;

    // async fn create_database(&self, params: &DatabaseCreateRequest) -> ControlPlaneResult<Database>;
    // async fn get_database(&self, id: Uuid) -> ControlPlaneResult<Database>;
    // async fn delete_database(&self, id: Uuid) -> ControlPlaneResult<()>;
    // async fn list_databases(&self) -> ControlPlaneResult<Vec<Database>>;

    // async fn create_table(&self, params: &TableCreateRequest) -> ControlPlaneResult<Table>;
    // async fn get_table(&self, id: Uuid) -> ControlPlaneResult<Table>;
    // async fn delete_table(&self, id: Uuid) -> ControlPlaneResult<()>;
    // async fn list_tables(&self) -> ControlPlaneResult<Vec<Table>>;

    async fn query(
        &self,
        session_id: &str,
        query: &str,
    ) -> ControlPlaneResult<(Vec<RecordBatch>, Vec<ColumnInfo>)>;

    async fn query_table(&self, session_id: &str, query: &str) -> ControlPlaneResult<String>;

    async fn upload_data_to_table(
        &self,
        session_id: &str,
        warehouse_id: &Uuid,
        database_name: &str,
        table_name: &str,
        data: Bytes,
        file_name: String,
    ) -> ControlPlaneResult<()>;

    async fn create_session(&self, session_id: String) -> ControlPlaneResult<()>;

    async fn delete_session(&self, session_id: String) -> ControlPlaneResult<()>;
}

pub struct ControlServiceImpl {
    storage_profile_repo: Arc<dyn StorageProfileRepository>,
    warehouse_repo: Arc<dyn WarehouseRepository>,
    df_sessions: Arc<RwLock<HashMap<String, SqlExecutor>>>,
}

impl ControlServiceImpl {
    pub fn new(
        storage_profile_repo: Arc<dyn StorageProfileRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
    ) -> Self {
        let df_sessions = Arc::new(RwLock::new(HashMap::new()));
        Self {
            storage_profile_repo,
            warehouse_repo,
            df_sessions,
        }
    }
}

#[async_trait]
impl ControlService for ControlServiceImpl {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_profile(
        &self,
        params: &StorageProfileCreateRequest,
    ) -> ControlPlaneResult<StorageProfile> {
        let profile = params
            .try_into()
            .context(crate::error::InvalidStorageProfileSnafu)?;

        if params.validate_credentials.unwrap_or_default() {
            self.validate_credentials(&profile).await?;
        }

        self.storage_profile_repo.create(&profile).await?;
        Ok(profile)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_profile(&self, id: Uuid) -> ControlPlaneResult<StorageProfile> {
        self.storage_profile_repo.get(id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_profile(&self, id: Uuid) -> ControlPlaneResult<()> {
        let wh = self.list_warehouses().await?;
        if wh.iter().any(|wh| wh.storage_profile_id == id) {
            return Err(ControlPlaneError::StorageProfileInUse { id });
        }
        self.storage_profile_repo.delete(id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn list_profiles(&self) -> ControlPlaneResult<Vec<StorageProfile>> {
        self.storage_profile_repo.list().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_session(&self, session_id: String) -> ControlPlaneResult<()> {
        let session_exists = { self.df_sessions.read().await.contains_key(&session_id) };
        if !session_exists {
            let sql_parser_dialect =
                env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());
            let state = SessionStateBuilder::new()
                .with_config(
                    SessionConfig::new()
                        .with_information_schema(true)
                        .with_option_extension(SessionParams::default())
                        .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect),
                )
                .with_default_features()
                .with_query_planner(Arc::new(IcebergQueryPlanner {}))
                .with_type_planner(Arc::new(CustomTypePlanner {}))
                .build();
            let ctx = SessionContext::new_with_state(state);
            let executor = SqlExecutor::new(ctx).context(crate::error::ExecutionSnafu)?;

            tracing::trace!("Acuiring write lock for df_sessions");
            let mut session_list_mut = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            session_list_mut.insert(session_id, executor);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_session(&self, session_id: String) -> ControlPlaneResult<()> {
        // TODO: Need to have a timeout for the lock
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(&session_id);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn validate_credentials(&self, profile: &StorageProfile) -> ControlPlaneResult<()> {
        if let Some(credentials) = profile.credentials.clone() {
            match credentials {
                Credentials::AccessKey(creds) => {
                    let profile_region = profile.region.clone().unwrap_or_default();
                    let credentials = StaticProvider::new_minimal(
                        creds.aws_access_key_id.clone(),
                        creds.aws_secret_access_key.clone(),
                    );
                    let region = Region::Custom {
                        name: profile_region.clone(),
                        endpoint: profile.endpoint.clone().unwrap_or_else(|| {
                            format!("https://s3.{profile_region}.amazonaws.com")
                        }),
                    };

                    let dispatcher =
                        HttpClient::new().context(crate::error::InvalidTLSConfigurationSnafu)?;
                    let client = S3Client::new_with(dispatcher, credentials, region);
                    let request = GetBucketAclRequest {
                        bucket: profile.bucket.clone().unwrap_or_default(),
                        expected_bucket_owner: None,
                    };
                    client.get_bucket_acl(request).await?;
                    Ok(())
                }
                Credentials::Role(_) => Err(ControlPlaneError::UnsupportedAuthenticationMethod {
                    method: credentials.to_string(),
                }),
            }
        } else {
            Err(ControlPlaneError::CredentialsValidationFailed)
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_warehouse(
        &self,
        params: &WarehouseCreateRequest,
    ) -> ControlPlaneResult<Warehouse> {
        // TODO: Check if storage profile exists
        // - Check if its valid
        // - Generate id, update created_at and updated_at
        // - Try create Warehouse from WarehouseCreateRequest
        let wh: Warehouse = params
            .try_into()
            .context(crate::error::InvalidCreateWarehouseSnafu)?;
        let _ = self.get_profile(wh.storage_profile_id).await?;
        self.warehouse_repo.create(&wh).await?;
        Ok(wh)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_warehouse_by_name(&self, name: String) -> ControlPlaneResult<Warehouse> {
        self.warehouse_repo.get_by_name(name.as_str()).await
    }

    async fn get_warehouse(&self, id: Uuid) -> ControlPlaneResult<Warehouse> {
        self.warehouse_repo.get(id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_warehouse(&self, id: Uuid) -> ControlPlaneResult<()> {
        // let db = self.list_databases().await?;
        // db.iter().filter(|db| db.warehouse_id == id).next().ok_or(Error::NotEmpty("Warehouse is in use".to_string()))?;
        self.warehouse_repo.delete(id).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn list_warehouses(&self) -> ControlPlaneResult<Vec<Warehouse>> {
        self.warehouse_repo.list().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
    ) -> ControlPlaneResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let sessions = self.df_sessions.read().await;
        let executor =
            sessions
                .get(session_id)
                .ok_or(error::ControlPlaneError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;

        let query = executor.preprocess_query(query);
        let statement = executor
            .parse_query(&query)
            .context(super::error::DataFusionSnafu)?;

        let table_ref = executor.get_table_path(&statement);
        let warehouse_name = table_ref
            .as_ref()
            .and_then(|table_ref| table_ref.catalog())
            .unwrap_or("")
            .to_string();

        let catalog_name: String = if warehouse_name.is_empty() {
            // try to get catalog name from session
            executor
                .get_session_variable("database")
                .unwrap_or_else(|| String::from("datafusion"))
        } else {
            let warehouse = self.get_warehouse_by_name(warehouse_name.clone()).await?;
            if executor.ctx.catalog(warehouse.name.as_str()).is_none() {
                let storage_profile = self.get_profile(warehouse.storage_profile_id).await?;

                let config = {
                    let mut config = Configuration::new();
                    config.base_path = "http://0.0.0.0:3000/catalog".to_string();
                    config
                };
                let object_store = storage_profile
                    .get_object_store_builder()
                    .context(crate::error::InvalidStorageProfileSnafu)?;
                let rest_client = RestCatalog::new(
                    Some(warehouse.id.to_string().as_str()),
                    config,
                    object_store,
                );

                let catalog = IcebergCatalog::new(Arc::new(rest_client), None).await?;
                if executor.ctx.catalog(warehouse.name.as_str()).is_none() {
                    executor
                        .ctx
                        .register_catalog(warehouse.name.clone(), Arc::new(catalog));
                }

                let object_store = storage_profile
                    .get_object_store()
                    .context(crate::error::InvalidStorageProfileSnafu)?;
                let endpoint_url = storage_profile
                    .get_object_store_endpoint_url()
                    .map_err(|_| ControlPlaneError::MissingStorageEndpointURL)?;
                executor
                    .ctx
                    .register_object_store(&endpoint_url, Arc::from(object_store));
            }
            warehouse.name
        };
        tracing::debug!("Catalog: {}", catalog_name);
        let records: Vec<RecordBatch> = executor
            .query(&query, catalog_name.as_str())
            .await
            .context(crate::error::ExecutionSnafu)?
            .into_iter()
            .collect::<Vec<_>>();
        // Add columns dbt metadata to each field
        convert_record_batches(records).context(crate::error::DataFusionQuerySnafu { query })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_table(&self, session_id: &str, query: &str) -> ControlPlaneResult<String> {
        let (records, _) = self.query(session_id, query).await?;
        let buf = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(buf);

        let record_refs: Vec<&RecordBatch> = records.iter().collect();
        writer
            .write_batches(&record_refs)
            .context(crate::error::ArrowSnafu)?;
        writer.finish().context(crate::error::ArrowSnafu)?;

        // Get the underlying buffer back,
        let buf = writer.into_inner();

        Ok(String::from_utf8(buf).context(crate::error::Utf8Snafu)?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        warehouse_id: &Uuid,
        database_name: &str,
        table_name: &str,
        data: Bytes,
        file_name: String,
    ) -> ControlPlaneResult<()> {
        let warehouse = self.get_warehouse(*warehouse_id).await?;
        let warehouse_name = warehouse.name.clone();
        let storage_profile = self.get_profile(warehouse.storage_profile_id).await?;
        let object_store = storage_profile
            .get_object_store()
            .context(crate::error::InvalidStorageProfileSnafu)?;
        let unique_file_id = Uuid::new_v4().to_string();

        let sessions = self.df_sessions.read().await;
        let executor =
            sessions
                .get(session_id)
                .ok_or(error::ControlPlaneError::MissingDataFusionSession {
                    id: session_id.to_string(),
                })?;

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_part = format!("{}/{}", warehouse.location, table_name);
        let path_string = format!("{table_part}/tmp/{unique_file_id}/{file_name}");

        let path = Path::from(path_string.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(crate::error::ObjectStoreSnafu)?;

        if executor.ctx.catalog(warehouse.name.as_str()).is_none() {
            // Create table from CSV
            let config = {
                let mut config = Configuration::new();
                config.base_path = "http://0.0.0.0:3000/catalog".to_string();
                config
            };
            let object_store_builder = storage_profile
                .get_object_store_builder()
                .context(crate::error::InvalidStorageProfileSnafu)?;
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
            .context(crate::error::ExecutionSnafu)?;

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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use super::*;
    use crate::error::ControlPlaneError;
    use crate::models::{
        AwsAccessKeyCredential, CloudProvider, Credentials, StorageProfileCreateRequest,
    };
    use crate::repository::InMemoryStorageProfileRepository;
    use crate::repository::InMemoryWarehouseRepository;
    use std::env;

    #[tokio::test]
    async fn test_create_warehouse_failed_no_storage_profile() {
        let storage_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);
        let new_uuid = Uuid::new_v4();
        let request = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: new_uuid,
            prefix: "test-prefix".to_string(),
        };

        let err = service.create_warehouse(&request).await;
        assert!(matches!(
            err,
            Err(ControlPlaneError::StorageProfileNotFound { id: _ })
        ));
    }

    #[tokio::test]
    async fn test_delete_nonempty_storage_profile() {
        let storage_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);

        let request = StorageProfileCreateRequest {
            r#type: CloudProvider::AWS,
            region: Some("us-west-2".to_string()),
            bucket: Some("my-bucket".to_string()),
            credentials: Some(Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            })),
            sts_role_arn: None,
            endpoint: None,
            validate_credentials: None,
        };
        let profile = service.create_profile(&request).await.unwrap();
        let request = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: profile.id,
            prefix: "test-prefix".to_string(),
        };

        let wh = service.create_warehouse(&request).await.unwrap();
        let result = service
            .delete_profile(wh.storage_profile_id)
            .await
            .unwrap_err();
        assert!(matches!(
            result,
            ControlPlaneError::StorageProfileInUse { id: _ }
        ));
    }

    fn storage_profile_req() -> StorageProfileCreateRequest {
        StorageProfileCreateRequest {
            r#type: CloudProvider::AWS,
            region: Some("us-west-2".to_string()),
            bucket: Some("my-bucket".to_string()),
            credentials: Some(Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            })),
            sts_role_arn: None,
            endpoint: Some(String::from("https://s3.us-east-2.amazonaws.com/")),
            validate_credentials: None,
        }
    }
    fn warehouse_req(name: &str, sp_id: Uuid) -> WarehouseCreateRequest {
        WarehouseCreateRequest {
            name: name.to_string(),
            storage_profile_id: sp_id,
            prefix: "prefix".to_string(),
        }
    }

    async fn _test_queries(
        storage_repo: Arc<dyn StorageProfileRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
    ) {
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);
        service
            .create_session("TEST_SESSION".to_string())
            .await
            .unwrap();
        service
            .query("TEST_SESSION", "SELECT 1")
            .await
            .expect("Scalar function should success!");

        let profile = service
            .create_profile(&storage_profile_req())
            .await
            .unwrap();

        service
            .create_warehouse(&warehouse_req("TEST_WAREHOUSE", profile.id))
            .await
            .expect("Should create warehouse");

        // Request error: error sending request for url
        // (http://0.0.0.0:3000/catalog/v1/<UUID>/namespaces)
        // error trying to connect: tcp connect error: Connection refused (os error 111)
        // Following code snippet is commented as of above error:
        //
        // service
        //     .query("CREATE SCHEMA IF NOT EXISTS TEST_WAREHOUSE.TEST_SCHEMA")
        //     .await
        //     .expect("Create schema should success!");
    }

    #[tokio::test]
    async fn test_inmemory_queries() {
        env::set_var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD", "true");
        env::set_var("SQL_PARSER_DIALECT", "snowflake");
        env::set_var("OBJECT_STORE_BACKEND", "file");
        _test_queries(
            Arc::new(InMemoryStorageProfileRepository::default()),
            Arc::new(InMemoryWarehouseRepository::default()),
        )
        .await;
    }
}
