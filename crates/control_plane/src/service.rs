use crate::error::{ControlPlaneError, ControlPlaneResult};
use crate::models::{ColumnInfo, Credentials, StorageProfile, StorageProfileCreateRequest};
use crate::models::{Warehouse, WarehouseCreateRequest};
use crate::repository::{StorageProfileRepository, WarehouseRepository};
use crate::utils::convert_record_batches;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::JsonArray;
use arrow_json::WriterBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::CsvReadOptions;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use runtime::datafusion::execution::SqlExecutor;
use runtime::datafusion::session::Session;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetBucketAclRequest, S3Client, S3};
use snafu::ResultExt;
use std::sync::Arc;
use url::Url;
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

    async fn query(&self, query: &str) -> ControlPlaneResult<(Vec<RecordBatch>, Vec<ColumnInfo>)>;

    async fn query_table(&self, query: &str) -> ControlPlaneResult<String>;

    async fn query_dbt(&self, query: &str) -> ControlPlaneResult<(String, Vec<ColumnInfo>)>;

    async fn upload_data_to_table(
        &self,
        warehouse_id: &Uuid,
        database_name: &str,
        table_name: &str,
        data: Bytes,
        file_name: String,
    ) -> ControlPlaneResult<()>;
}

pub struct ControlServiceImpl {
    storage_profile_repo: Arc<dyn StorageProfileRepository>,
    warehouse_repo: Arc<dyn WarehouseRepository>,
    executor: SqlExecutor,
}

impl ControlServiceImpl {
    pub fn new(
        storage_profile_repo: Arc<dyn StorageProfileRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
    ) -> Self {
        let session = Session::default();
        #[allow(clippy::unwrap_used)]
        let executor = SqlExecutor::new(session.ctx).unwrap();
        Self {
            storage_profile_repo,
            warehouse_repo,
            executor,
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn validate_credentials(&self, profile: &StorageProfile) -> ControlPlaneResult<()> {
        match profile.credentials.clone() {
            Credentials::AccessKey(creds) => {
                let profile_region = profile.region.clone();
                let credentials = StaticProvider::new_minimal(
                    creds.aws_access_key_id.clone(),
                    creds.aws_secret_access_key.clone(),
                );
                let region = Region::Custom {
                    name: profile_region.clone(),
                    endpoint: profile
                        .endpoint
                        .clone()
                        .unwrap_or_else(|| format!("https://s3.{profile_region}.amazonaws.com")),
                };

                let dispatcher =
                    HttpClient::new().context(crate::error::InvalidTLSConfigurationSnafu)?;
                let client = S3Client::new_with(dispatcher, credentials, region);
                let request = GetBucketAclRequest {
                    bucket: profile.bucket.clone(),
                    expected_bucket_owner: None,
                };
                client.get_bucket_acl(request).await?;
                Ok(())
            }
            Credentials::Role(_) => Err(ControlPlaneError::UnsupportedAuthenticationMethod {
                method: profile.credentials.to_string(),
            }),
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
    async fn query(&self, query: &str) -> ControlPlaneResult<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let query = self.executor.preprocess_query(query);
        let statement = self
            .executor
            .parse_query(&query)
            .context(super::error::DataFusionSnafu)?;

        let table_ref = self.executor.get_table_path(&statement);
        let warehouse_name = table_ref
            .as_ref()
            .and_then(|table_ref| table_ref.catalog())
            .unwrap_or("")
            .to_string();

        let (catalog_name, warehouse_location): (String, String) = if warehouse_name.is_empty() {
            (String::from("datafusion"), String::new())
        } else {
            let warehouse = self.get_warehouse_by_name(warehouse_name.clone()).await?;
            if self.executor.ctx.catalog(warehouse.name.as_str()).is_none() {
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
                if self.executor.ctx.catalog(warehouse.name.as_str()).is_none() {
                    self.executor
                        .ctx
                        .register_catalog(warehouse.name.clone(), Arc::new(catalog));
                }

                let object_store = storage_profile
                    .get_object_store()
                    .context(crate::error::InvalidStorageProfileSnafu)?;
                let endpoint_url = storage_profile
                    .get_object_store_endpoint_url()
                    .map_err(|_| ControlPlaneError::MissingStorageEndpointURL)?;
                self.executor
                    .ctx
                    .register_object_store(&endpoint_url, Arc::from(object_store));
            }
            (warehouse.name, warehouse.location)
        };

        let records: Vec<RecordBatch> = self
            .executor
            .query(&query, catalog_name.as_str(), warehouse_location.as_str())
            .await
            .context(crate::error::ExecutionSnafu)?
            .into_iter()
            .collect::<Vec<_>>();
        // Add columns dbt metadata to each field
        convert_record_batches(records).context(crate::error::DataFusionQuerySnafu { query })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_table(&self, query: &str) -> ControlPlaneResult<String> {
        let (records, _) = self.query(query).await?;

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
    async fn query_dbt(&self, query: &str) -> ControlPlaneResult<(String, Vec<ColumnInfo>)> {
        let (records, columns) = self.query(query).await?;

        // THIS CODE RELATED TO ARROW FORMAT
        //////////////////////////////////////

        // fn roundtrip_ipc_stream(rb: &RecordBatch) -> RecordBatch {
        //     let mut buf = Vec::new();
        //     let mut writer = StreamWriter::try_new(&mut buf, rb.schema_ref()).unwrap();
        //     writer.write(rb).unwrap();
        //     writer.finish().unwrap();
        //     drop(writer);
        //
        //     let mut reader = StreamReader::try_new(std::io::Cursor::new(buf), None).unwrap();
        //     reader.next().unwrap().unwrap()
        // }
        //
        // println!("agahaha {:?}", roundtrip_ipc_stream(&records[0]));

        // let mut buffer = Vec::new();
        // let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        // let mut stream_writer =
        //     StreamWriter::try_new_with_options(&mut buffer, &records[0].schema_ref(), options)
        //         .unwrap();
        // stream_writer.write(&records[0]).unwrap();
        // stream_writer.finish().unwrap();
        // drop(stream_writer);

        // // Try to add flatbuffer verification
        // println!("{:?}", buffer.len());
        // let base64 = general_purpose::STANDARD.encode(buffer);
        // Ok((base64, columns))
        // let encoded = general_purpose::STANDARD.decode(res.clone()).unwrap();
        //
        // let mut verifier = Verifier::new(&VerifierOptions::default(), &encoded);
        // let mut builder = FlatBufferBuilder::new();
        // let res = general_purpose::STANDARD.encode(buf);
        //////////////////////////////////////

        // We use json format since there is a bug between arrow and nanoarrow
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
        Ok((
            String::from_utf8(buf).context(crate::error::Utf8Snafu)?,
            columns,
        ))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn upload_data_to_table(
        &self,
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

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_part = format!("{}/{}", warehouse.location, table_name);
        let path_string = format!("{table_part}/tmp/{unique_file_id}/{file_name}");

        let path = Path::from(path_string.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .context(crate::error::ObjectStoreSnafu)?;

        if self.executor.ctx.catalog(warehouse.name.as_str()).is_none() {
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
            self.executor
                .ctx
                .register_catalog(warehouse.name.clone(), Arc::new(catalog));
        }

        // Register CSV file as a table
        let storage_endpoint_url = storage_profile
            .endpoint
            .as_ref()
            .ok_or(ControlPlaneError::MissingStorageEndpointURL)?;

        let path_string = match &storage_profile.credentials {
            Credentials::AccessKey(_) => {
                // If the storage profile is AWS S3, modify the path_string with the S3 prefix
                format!("{storage_endpoint_url}/{path_string}")
            }
            Credentials::Role(_) => path_string,
        };
        let endpoint_url = Url::parse(storage_endpoint_url).context(
            crate::error::InvalidStorageEndpointURLSnafu {
                url: storage_endpoint_url,
            },
        )?;
        self.executor
            .ctx
            .register_object_store(&endpoint_url, Arc::from(object_store));
        self.executor
            .ctx
            .register_csv(table_name, path_string, CsvReadOptions::new())
            .await?;

        let insert_query = format!(
            "INSERT INTO {warehouse_name}.{database_name}.{table_name} SELECT * FROM {table_name}"
        );
        self.executor
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
#[allow(clippy::unwrap_used)]
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
            region: "us-west-2".to_string(),
            bucket: "my-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            }),
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
            region: "us-west-2".to_string(),
            bucket: "my-bucket".to_string(),
            credentials: Credentials::AccessKey(AwsAccessKeyCredential {
                aws_access_key_id: "my-access-key".to_string(),
                aws_secret_access_key: "my-secret-access-key".to_string(),
            }),
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
            .query("SELECT 1")
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
