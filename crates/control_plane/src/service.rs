use crate::error::{extract_error_message, Error, Result};
use crate::models::{ColumnInfo, Credentials, StorageProfile, StorageProfileCreateRequest};
use crate::models::{Warehouse, WarehouseCreateRequest};
use crate::repository::{StorageProfileRepository, WarehouseRepository};
use crate::sql::functions::common::convert_record_batches;
use crate::sql::sql::SqlExecutor;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::MetadataVersion;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine};
use bytes::Bytes;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, SessionConfig};
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use flatbuffers::{FlatBufferBuilder, Verifier, VerifierOptions};
use iceberg_rest_catalog::apis::configuration::Configuration;
use iceberg_rest_catalog::catalog::RestCatalog;
use icelake::TableIdentifier;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetBucketAclRequest, S3Client, S3};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

#[async_trait]
pub trait ControlService: Send + Sync {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile>;
    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile>;
    async fn delete_profile(&self, id: Uuid) -> Result<()>;
    async fn list_profiles(&self) -> Result<Vec<StorageProfile>>;
    async fn validate_credentials(&self, profile: &StorageProfile) -> Result<()>;

    async fn create_warehouse(&self, params: &WarehouseCreateRequest) -> Result<Warehouse>;
    async fn get_warehouse(&self, id: Uuid) -> Result<Warehouse>;
    async fn delete_warehouse(&self, id: Uuid) -> Result<()>;
    async fn list_warehouses(&self) -> Result<Vec<Warehouse>>;

    // async fn create_database(&self, params: &DatabaseCreateRequest) -> Result<Database>;
    // async fn get_database(&self, id: Uuid) -> Result<Database>;
    // async fn delete_database(&self, id: Uuid) -> Result<()>;
    // async fn list_databases(&self) -> Result<Vec<Database>>;

    // async fn create_table(&self, params: &TableCreateRequest) -> Result<Table>;
    // async fn get_table(&self, id: Uuid) -> Result<Table>;
    // async fn delete_table(&self, id: Uuid) -> Result<()>;
    // async fn list_tables(&self) -> Result<Vec<Table>>;

    async fn query(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        table_name: &String,
        query: &String,
    ) -> Result<(Vec<RecordBatch>, Vec<ColumnInfo>)>;

    async fn query_table(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        table_name: &String,
        query: &String,
    ) -> Result<String>;

    async fn query_dbt(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        table_name: &String,
        query: &String,
    ) -> Result<(String, Vec<ColumnInfo>)>;

    async fn upload_data_to_table(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        table_name: &String,
        data: Bytes,
        file_name: String,
    ) -> Result<()>;
}

pub struct ControlServiceImpl {
    storage_profile_repo: Arc<dyn StorageProfileRepository>,
    warehouse_repo: Arc<dyn WarehouseRepository>,
}

impl ControlServiceImpl {
    pub fn new(
        storage_profile_repo: Arc<dyn StorageProfileRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
    ) -> Self {
        Self {
            storage_profile_repo,
            warehouse_repo,
        }
    }
}

#[async_trait]
impl ControlService for ControlServiceImpl {
    async fn create_profile(&self, params: &StorageProfileCreateRequest) -> Result<StorageProfile> {
        let profile = params.try_into()?;

        if params.validate_credentials.unwrap_or_default() {
            self.validate_credentials(&profile).await?;
        }

        self.storage_profile_repo.create(&profile).await?;
        Ok(profile)
    }

    async fn get_profile(&self, id: Uuid) -> Result<StorageProfile> {
        self.storage_profile_repo.get(id).await
    }

    async fn delete_profile(&self, id: Uuid) -> Result<()> {
        let wh = self.list_warehouses().await?;
        if wh.iter().any(|wh| wh.storage_profile_id == id) {
            return Err(Error::NotEmpty("Storage profile is in use".to_string()));
        }
        self.storage_profile_repo.delete(id).await
    }

    async fn list_profiles(&self) -> Result<Vec<StorageProfile>> {
        self.storage_profile_repo.list().await
    }

    async fn validate_credentials(&self, profile: &StorageProfile) -> Result<()> {
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
                        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", profile_region)),
                };

                let dispatcher = HttpClient::new().expect("Failed to create HTTP client");
                let client = S3Client::new_with(dispatcher, credentials, region);
                let request = GetBucketAclRequest {
                    bucket: profile.bucket.clone(),
                    expected_bucket_owner: None,
                };
                client
                    .get_bucket_acl(request)
                    .await
                    .map_err(|e| Error::InvalidCredentials(extract_error_message(&e).unwrap()))?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn create_warehouse(&self, params: &WarehouseCreateRequest) -> Result<Warehouse> {
        // TODO: Check if storage profile exists
        // - Check if its valid
        // - Generate id, update created_at and updated_at
        // - Try create Warehouse from WarehouseCreateRequest
        let wh: Warehouse = params.try_into()?;
        let _ = self.get_profile(wh.storage_profile_id).await?;
        self.warehouse_repo.create(&wh).await?;
        Ok(wh)
    }

    async fn get_warehouse(&self, id: Uuid) -> Result<Warehouse> {
        self.warehouse_repo.get(id).await
    }

    async fn delete_warehouse(&self, id: Uuid) -> Result<()> {
        // let db = self.list_databases().await?;
        // db.iter().filter(|db| db.warehouse_id == id).next().ok_or(Error::NotEmpty("Warehouse is in use".to_string()))?;
        self.warehouse_repo.delete(id).await
    }

    async fn list_warehouses(&self) -> Result<Vec<Warehouse>> {
        self.warehouse_repo.list().await
    }

    async fn query(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        _table_name: &String,
        query: &String,
    ) -> Result<(Vec<RecordBatch>, Vec<ColumnInfo>)> {
        let warehouse = self.get_warehouse(*warehouse_id).await?;
        let storage_profile = self.get_profile(warehouse.storage_profile_id).await?;

        let config = {
            let mut config = Configuration::new();
            config.base_path = "http://0.0.0.0:3000/catalog".to_string();
            config
        };
        let rest_client = RestCatalog::new(
            Some(warehouse_id.to_string().as_str()),
            config,
            storage_profile.get_object_store_builder(),
        );
        let catalog = IcebergCatalog::new(Arc::new(rest_client), None).await?;

        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let catalog_name = warehouse.name.clone();
        ctx.register_catalog(catalog_name.clone(), Arc::new(catalog));

        let provider = ctx.catalog(catalog_name.clone().as_str()).unwrap();
        let schemas = provider.schema_names();
        println!("{schemas:?}");

        let tables = provider.schema(database_name).unwrap().table_names();
        println!("{tables:?}");

        let records: Vec<RecordBatch> = SqlExecutor::new(ctx)
            .query(query, &catalog_name.clone().to_string())
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        // Add columns dbt metadata to each field
        convert_record_batches(records).map_err(|e| Error::DataFusionError(e.to_string()))
    }

    async fn query_table(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        _table_name: &String,
        query: &String,
    ) -> Result<String> {
        let (records, _) = self
            .query(warehouse_id, database_name, _table_name, query)
            .await?;
        println!("{records:?}");

        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        let record_refs: Vec<&RecordBatch> = records.iter().collect();
        writer.write_batches(&record_refs).unwrap();
        writer.finish().unwrap();

        // Get the underlying buffer back,
        let buf = writer.into_inner();

        Ok(String::from_utf8(buf).unwrap())
    }

    async fn query_dbt(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        _table_name: &String,
        query: &String,
    ) -> Result<(String, Vec<ColumnInfo>)> {
        let (records, columns) = self
            .query(warehouse_id, database_name, _table_name, query)
            .await?;

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

        let mut buffer = Vec::new();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut stream_writer = StreamWriter::try_new_with_options(
            &mut buffer, &records[0].schema_ref(), options).unwrap();
        stream_writer.write(&records[0]).unwrap();
        stream_writer.finish().unwrap();
        drop(stream_writer);

        // Try to add flatbuffer verification
        println!("{:?}", buffer.len());
        let res = general_purpose::STANDARD.encode(buffer);
        let encoded = general_purpose::STANDARD.decode(res.clone()).unwrap();

        let mut verifier = Verifier::new(&VerifierOptions::default(), &encoded);
        let mut builder = FlatBufferBuilder::new();


        Ok((res, columns))
    }

    async fn upload_data_to_table(
        &self,
        warehouse_id: &Uuid,
        database_name: &String,
        table_name: &String,
        data: Bytes,
        file_name: String,
    ) -> Result<()> {
        println!("{:?}", warehouse_id);

        let warehouse = self.get_warehouse(*warehouse_id).await?;
        let storage_profile = self.get_profile(warehouse.storage_profile_id).await?;
        let object_store = storage_profile.get_object_store();
        let unique_file_id = Uuid::new_v4().to_string();

        // this path also computes inside catalog service (create_table)
        // TODO need to refactor the code so this path calculation is in one place
        let table_part = format!("{}/{}", warehouse.location, table_name);
        let path_string = format!("{table_part}/tmp/{unique_file_id}/{file_name}");

        let path = Path::from(path_string.clone());
        object_store
            .put(&path, PutPayload::from(data))
            .await
            .unwrap();

        let ctx = SessionContext::new();

        let path_string = match &storage_profile.credentials {
            Credentials::AccessKey(_) => {
                // If the storage profile is AWS S3, modify the path_string with the S3 prefix
                format!(
                    "{}/{}",
                    storage_profile.clone().endpoint.unwrap().as_str(),
                    path_string
                )
            }
            _ => path_string,
        };
        let endpoint_url = Url::parse(storage_profile.endpoint.clone().unwrap().as_str())
            .map_err(|e| Error::DataFusionError(format!("Invalid endpoint URL: {}", e)))?;
        ctx.register_object_store(&endpoint_url, Arc::from(object_store));

        // println!("{:?}", data);
        // Commented code is writing with iceberg-rust-jankaul
        // Let it sit here just in case
        //////////////////////////////////////

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
        // let catalog = IcebergCatalog::new(Arc::new(rest_client), None)
        //     .await
        //     .unwrap();
        //
        // let ctx = SessionContext::new();
        // ctx.register_catalog("catalog", Arc::new(catalog));
        //
        // let provider = ctx.catalog("catalog").unwrap();
        //
        // let tables = provider.schema(database_name).unwrap().table_names();
        // println!("{tables:?}");
        // let input = ctx.read_batches(data).unwrap();
        // input.write_table(format!("catalog.`{database_name}`.`{table_name}`").as_str(),
        //                   DataFrameWriteOptions::default()).await.unwrap();

        //////////////////////////////////////

        let config = {
            HashMap::from([
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                (
                    "iceberg.catalog.demo.warehouse".to_string(),
                    warehouse_id.to_string(),
                ),
                ("iceberg.catalog.name".to_string(), "demo".to_string()),
                (
                    "iceberg.catalog.demo.uri".to_string(),
                    "http://0.0.0.0:3000/catalog".to_string(),
                ),
                (
                    "iceberg.table.io.region".to_string(),
                    storage_profile.region.to_string(),
                ),
                (
                    "iceberg.table.io.endpoint".to_string(),
                    storage_profile.endpoint.unwrap().to_string(),
                ),
                // (
                //     "iceberg.table.io.bucket".to_string(),
                //     "examples".to_string(),
                // ),
                // (
                //     "iceberg.table.io.access_key_id".to_string(),
                //     "minioadmin".to_string(),
                // ),
                // (
                //     "iceberg.table.io.secret_access_key".to_string(),
                //     "minioadmin".to_string(),
                // ),
            ])
        };
        let catalog = icelake::catalog::load_catalog(&config).await?;
        let table_ident = TableIdentifier::new(vec![database_name, table_name])?;
        let mut table = catalog.load_table(&table_ident).await?;
        let table_schema = table.current_arrow_schema()?;
        println!("{:?}", table.table_name());

        let df = ctx
            .read_csv(path_string, CsvReadOptions::new().schema(&*table_schema))
            .await?;
        let data = df.collect().await?;

        let builder = table.writer_builder()?.rolling_writer_builder(None)?;
        let mut writer = table
            .writer_builder()?
            .build_append_only_writer(builder)
            .await?;

        for r in data {
            writer.write(&r).await?;
        }

        let res: Vec<icelake::types::DataFile> = writer.close().await?;
        let mut txn = icelake::transaction::Transaction::new(&mut table);
        txn.append_data_file(res);
        txn.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::models::{
        AwsAccessKeyCredential, CloudProvider, Credentials, StorageProfileCreateRequest,
    };
    use crate::repository::InMemoryStorageProfileRepository;
    use crate::repository::InMemoryWarehouseRepository;

    #[tokio::test]
    async fn test_create_warehouse_failed_no_storage_profile() {
        let storage_repo = Arc::new(InMemoryStorageProfileRepository::default());
        let warehouse_repo = Arc::new(InMemoryWarehouseRepository::default());
        let service = ControlServiceImpl::new(storage_repo, warehouse_repo);
        let request = WarehouseCreateRequest {
            name: "test-warehouse".to_string(),
            storage_profile_id: Uuid::new_v4(),
            prefix: "test-prefix".to_string(),
        };

        let err = service.create_warehouse(&request).await.unwrap_err();
        assert!(matches!(err, Error::ErrNotFound));
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
        assert!(matches!(result, Error::NotEmpty(_)));
    }
}
