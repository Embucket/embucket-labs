use crate::models::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use chrono::Utc;
use core_history::store::SlateDBHistoryStore;
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::Volume as MetastoreVolume;
use core_utils::Db;
use futures::future::join_all;
use object_store::ObjectStore;
use object_store::{local::LocalFileSystem, aws::AmazonS3Builder, aws::S3ConditionalPut, aws::AmazonS3ConfigKey};
use slatedb::{Db as SlateDb, config::DbOptions};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use dotenv::dotenv;
use core_metastore::{FileVolume, S3Volume};
use core_metastore::models::volumes::AwsCredentials;
use core_metastore::models::volumes::AwsAccessKeyCredentials;


// Set envs, and add to .env
// # Object store minio
// E2E_STORE_AWS_ACCESS_KEY_ID=
// E2E_STORE_AWS_SECRET_ACCESS_KEY=
// E2E_STORE_AWS_REGION=us-east-1
// E2E_STORE_AWS_BUCKET=e2e-store
// E2E_STORE_AWS_ENDPOINT=http://localhost:9000

// # User data on minio
// AWS_ACCESS_KEY_ID=
// AWS_SECRET_ACCESS_KEY=
// AWS_REGION=us-east-1
// AWS_BUCKET=tables-data
// AWS_ENDPOINT=http://localhost:9000


pub const TEST_SESSION_ID: &str = "test_session_id";

pub const TEST_VOLUME_MEMORY: (&str, &str) = ("test_volume_memory", "test_database_memory");
pub const TEST_VOLUME_FILE: (&str, &str) = ("test_volume_file", "test_database_file");
pub const TEST_VOLUME_S3: (&str, &str) = ("test_volume_s3", "test_database_s3");

pub const TEST_DATABASE_NAME: &str = "embucket";
pub const TEST_SCHEMA_NAME: &str = "public";

pub fn s3_volume() -> S3Volume {
    let s3_builder = AmazonS3Builder::from_env(); //.build().expect("Failed to load S3 credentials");
    let access_key_id = s3_builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId)
        .expect("AWS_ACCESS_KEY_ID is not set");
    let secret_access_key = s3_builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey)
        .expect("AWS_SECRET_ACCESS_KEY is not set");
    let region = s3_builder.get_config_value(&AmazonS3ConfigKey::Region)
        .expect("AWS_REGION is not set");
    let bucket = s3_builder.get_config_value(&AmazonS3ConfigKey::Bucket)
        .expect("AWS_BUCKET is not set");
    let endpoint = s3_builder.get_config_value(&AmazonS3ConfigKey::Endpoint)
        .expect("AWS_ENDPOINT is not set");
    S3Volume {
        region: Some(region),
        bucket: Some(bucket),
        endpoint: Some(endpoint),
        credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: access_key_id,
            aws_secret_access_key: secret_access_key,
        })),
    }
}

#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    pub s3_builder: AmazonS3Builder
}

impl S3ObjectStore {
    pub fn from_prefixed_env(prefix: &str) -> Self {
        let prefix_case = prefix.to_ascii_uppercase();
        let no_access_key_var = format!("{prefix}_AWS_ACCESS_KEY_ID is not set");
        let no_secret_key_var = format!("{prefix}_AWS_SECRET_ACCESS_KEY is not set");
        let no_region_var = format!("{prefix}_AWS_REGION is not set");
        let no_bucket_var = format!("{prefix}_AWS_BUCKET is not set");
        let no_endpoint_var = format!("{prefix}_AWS_ENDPOINT is not set");

        let region = std::env::var(format!("{}_AWS_REGION", prefix_case))
            .expect(&no_region_var);
        //.unwrap_or("us-east-1".into());
        let access_key = std::env::var(format!("{}_AWS_ACCESS_KEY_ID", prefix_case))
            .expect(&no_access_key_var);
        let secret_key = std::env::var(format!("{}_AWS_SECRET_ACCESS_KEY", prefix_case))
            .expect(&no_secret_key_var);
        let endpoint = std::env::var(format!("{}_AWS_ENDPOINT", prefix_case))
            .expect(&no_endpoint_var);
        let bucket = std::env::var(format!("{}_AWS_BUCKET", prefix))
            .expect(&no_bucket_var);

        Self { 
            s3_builder: AmazonS3Builder::new()
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_region(region)
                .with_endpoint(&endpoint)
                .with_allow_http(true)
                .with_bucket_name(bucket)
                .with_conditional_put(S3ConditionalPut::ETagMatch),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ObjectStoreType {
    Memory,
    File(String),
    S3(S3ObjectStore),
}

impl ObjectStoreType {
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self {
            Self::Memory => Arc::new(object_store::memory::InMemory::new()),
            Self::File(suffix) => {
                let mut temp_dir = env::temp_dir();
                temp_dir.push(format!("object_store_{suffix}"));
                Arc::new(object_store(temp_dir.as_path()))
            }
            Self::S3(s3_object_store) => {
                s3_object_store.s3_builder.clone().build()
                    .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                    .expect("Failed to create S3 client")
            }
        }
    }

    pub async fn db(&self) -> Db {
        Db::new(Arc::new(
            SlateDb::open_with_opts(
                object_store::path::Path::from("metadata"),
                DbOptions::default(),
                self.object_store(),
            )
            .await
            .expect("Failed to start Slate DB"),
        ))
    }
}



#[allow(clippy::unwrap_used, clippy::as_conversions)]
#[must_use]
pub fn object_store(path: &Path) -> Arc<dyn ObjectStore> {
    if !path.exists() || !path.is_dir() {
        fs::create_dir(path).unwrap();
    }
    LocalFileSystem::new_with_prefix(path)
        .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
        .expect("Failed to create object store")
}

pub async fn create_executor(object_store_type: &ObjectStoreType, user_data_dir: &Path) -> CoreExecutionService {
    let db = object_store_type.db().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    );

    // TODO: Move volume creation to prerequisite_statements after we can create volume with SQL
    // Now, just ignore volume creating error, as we create multiple executors

    // ignore errors when creating volume, as it could be created in previous run
    let _ = metastore
        .create_volume(
            &TEST_VOLUME_MEMORY.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_MEMORY.0.to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await;

    let _ = metastore
        .create_volume(
            &TEST_VOLUME_FILE.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_FILE.0.to_string(),
                core_metastore::VolumeType::File(FileVolume { path: user_data_dir.display().to_string() }),
            ),
        )
        .await;

    let _ = metastore
        .create_volume(
            &TEST_VOLUME_S3.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_S3.0.to_string(),
                core_metastore::VolumeType::S3(s3_volume()),
            ),
        )
        .await;

    execution_svc
        .create_session(TEST_SESSION_ID.to_string())
        .await
        .expect("Failed to create session");

    execution_svc
}

async fn exec_statements_with_multiple_writers(
    n_writers: usize,
    user_data_dir: &Path,
    object_store_type: ObjectStoreType,
    prerequisite_statements: Vec<String>,
    test_statements: Vec<String>,
) -> bool {
    assert!(n_writers > 1);

    let mut execution_svc_writers: Vec<CoreExecutionService> = vec![];
    for _ in 0..n_writers {
        execution_svc_writers.push(create_executor(&object_store_type, &user_data_dir).await);
    }

    // Use single writer for prepare statements
    for (idx, statement) in prerequisite_statements.iter().enumerate() {
        if let Err(err) = execution_svc_writers[0]
            .query(TEST_SESSION_ID, statement, QueryContext::default())
            .await
        {
            panic!("Prepare sql statement #{idx} execution error: {err}");
        }
    }

    let mut passed = true;
    for (idx, statement) in test_statements.iter().enumerate() {
        let mut futures = vec![];
        for execution_svc in execution_svc_writers.iter().take(n_writers) {
            futures.push(execution_svc.query(TEST_SESSION_ID, statement, QueryContext::default()));
        }

        let results = join_all(futures).await;

        let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
        let oks: Vec<_> = oks.into_iter().map(Result::unwrap).collect();
        let errs: Vec<_> = errs.into_iter().map(Result::unwrap_err).collect();
        if oks.len() != 1 || errs.len() != n_writers - 1 {
            eprintln!("FAIL Test statement #{idx}: ({statement})");
            eprintln!("ok_results: {oks:#?}, err_results: {errs:#?}");
            passed = false;
        }
        else {
            let one_line_statement = statement.split('\n').nth(0).unwrap_or(statement);
            eprintln!("PASSED Test statement #{idx}: {one_line_statement}");
        }
    }
    passed
}

fn prepare_statements(raw_statements: &[&str], volume_name: &str, database_name: &str) -> Vec<String> {
    raw_statements
        .into_iter()
        .map(|statement| statement.replace("__VOLUME__", volume_name))
        .map(|statement| statement.replace("__DATABASE__", database_name))
        .map(|statement| statement.replace("__SCHEMA__", TEST_SCHEMA_NAME))
        .collect()
}
    

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_with_multiple_writers() {
    dotenv().ok();

    let volumes_list = vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3];

    let n_writers = 2;
    if let Some(nano_timestamp) = Utc::now().timestamp_nanos_opt() {
        let object_store_prefix = nano_timestamp.to_string();
        let storages = vec![
            ObjectStoreType::Memory,
            ObjectStoreType::File(object_store_prefix),
            ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
        ];
        let mut passed = true;
        for storage in storages {
            for (volume, database) in &volumes_list {
                let mut user_data_dir = env::temp_dir();
                user_data_dir.push(format!("user-data-{}", Utc::now().timestamp_nanos_opt().unwrap()));
                let user_data_dir = user_data_dir.as_path();
                eprintln!("Testing with storage: {storage:?}, volume: {volume}");
                passed = exec_statements_with_multiple_writers(n_writers, user_data_dir,
                    storage.clone(),
                    prepare_statements(&PREREQUISITE_STATEMENTS, volume, database),
                    prepare_statements(&TEST_STATEMENTS, volume, database)).await && passed;   
            }
        }
        assert!(passed);
    }
}

const PREREQUISITE_STATEMENTS: [&str; 2] = [
    "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
    "CREATE SCHEMA __DATABASE__.__SCHEMA__",
];
const TEST_STATEMENTS: [&str; 5] = [
    "CREATE TABLE __DATABASE__.__SCHEMA__.all_snowflake_types (
        -- Numeric Types
        col_number NUMBER,
        col_decimal DECIMAL(10,2),
        col_numeric NUMERIC(10,2),
        col_int INT,
        col_integer INTEGER,
        col_bigint BIGINT,
        col_smallint SMALLINT,
        col_float FLOAT,
        col_float4 FLOAT4,
        col_float8 FLOAT8,
        col_double DOUBLE,
        col_double_precision DOUBLE PRECISION,
        col_real REAL,

        -- String Types
        col_char CHAR(10),
        -- col_character CHARACTER(10),
        col_varchar VARCHAR(255),
        col_string STRING,
        col_text TEXT,

        -- Boolean
        col_boolean BOOLEAN,

        -- Date & Time Types
        col_date DATE,
        -- col_time TIME,
        col_timestamp TIMESTAMP,
        col_timestamp_ltz TIMESTAMP_LTZ,
        col_timestamp_ntz TIMESTAMP_NTZ,
        col_timestamp_tz TIMESTAMP_TZ,
        col_datetime DATETIME,

        -- Semi-structured
        col_variant VARIANT,
        col_object OBJECT,
        col_array ARRAY,

        -- Binary
        col_binary BINARY,
        col_varbinary VARBINARY

        -- Geography (optional feature)
        -- col_geography GEOGRAPHY
    )",
    "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
    // Seems alter table is not working
    // "ALTER TABLE __DATABASE__.__SCHEMA__.hello ADD COLUMN c5 VARCHAR",
    "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
    (100, 'Alice', 'foo'),
    (200, 'Bob', 'bar'),
    (300, 'Charlie', 'baz'),
    (400, 'Diana', 'qux'),
    (500, 'Eve', 'quux');",
    "DELETE FROM __DATABASE__.__SCHEMA__.hello WHERE c5 LIKE '%ux%'",
    "ALTER TABLE __DATABASE__.__SCHEMA__.hello DROP COLUMN c5",
];
