#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
use crate::models::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use chrono::Utc;
use core_history::store::SlateDBHistoryStore;
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::Volume as MetastoreVolume;
use core_metastore::models::volumes::AwsAccessKeyCredentials;
use core_metastore::models::volumes::AwsCredentials;
use core_metastore::{FileVolume, S3Volume};
use core_utils::Db;
use futures::future::join_all;
use object_store::ObjectStore;
use object_store::{
    aws::AmazonS3Builder, aws::AmazonS3ConfigKey, aws::S3ConditionalPut, local::LocalFileSystem,
};
use slatedb::{Db as SlateDb, config::DbOptions};
use snafu::ResultExt;
use snafu::{Location, Snafu};
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    Slatedb {
        source: slatedb::SlateDBError,
        #[snafu(implicit)]
        location: Location,
    },
    ObjectStore {
        source: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Query '{query}' execution error: {}", source))]
    Execution {
        query: String,
        source: crate::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

// Set envs, and add to .env
// # Object store on aws / minio
// E2E_STORE_AWS_ACCESS_KEY_ID=
// E2E_STORE_AWS_SECRET_ACCESS_KEY=
// E2E_STORE_AWS_REGION=us-east-1
// E2E_STORE_AWS_BUCKET=e2e-store
// E2E_STORE_AWS_ENDPOINT=http://localhost:9000

// # User data on aws / minio
// AWS_ACCESS_KEY_ID=
// AWS_SECRET_ACCESS_KEY=
// AWS_REGION=us-east-1
// AWS_BUCKET=tables-data
// AWS_ENDPOINT=http://localhost:9000

pub const TEST_SESSION_ID1: &str = "test_session_id1";
pub const TEST_SESSION_ID2: &str = "test_session_id2";

pub const TEST_VOLUME_MEMORY: (&str, &str) = ("volume_memory", "database_in_memory");
pub const TEST_VOLUME_FILE: (&str, &str) = ("volume_file", "database_in_file");
pub const TEST_VOLUME_S3: (&str, &str) = ("volume_s3", "database_in_s3");

pub const TEST_DATABASE_NAME: &str = "embucket";
pub const TEST_SCHEMA_NAME: &str = "public";

#[must_use]
pub fn test_suffix() -> String {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis())
        .to_string()
}

#[must_use]
pub fn s3_volume() -> S3Volume {
    let s3_builder = AmazonS3Builder::from_env(); //.build().expect("Failed to load S3 credentials");
    let access_key_id = s3_builder
        .get_config_value(&AmazonS3ConfigKey::AccessKeyId)
        .expect("AWS_ACCESS_KEY_ID is not set");
    let secret_access_key = s3_builder
        .get_config_value(&AmazonS3ConfigKey::SecretAccessKey)
        .expect("AWS_SECRET_ACCESS_KEY is not set");
    let region = s3_builder
        .get_config_value(&AmazonS3ConfigKey::Region)
        .expect("AWS_REGION is not set");
    let bucket = s3_builder
        .get_config_value(&AmazonS3ConfigKey::Bucket)
        .expect("AWS_BUCKET is not set");
    let endpoint = s3_builder
        .get_config_value(&AmazonS3ConfigKey::Endpoint)
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

pub type TestPlan = Vec<ParallelTest>;

pub struct ParallelTest(pub Vec<TestQuery>);

pub struct TestQuery {
    pub sqls: Vec<&'static str>,
    pub executor: Arc<ExecutorWithObjectStore>,
    pub session_id: &'static str,
    pub expected_res: bool,
}

#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    pub s3_builder: AmazonS3Builder,
}

impl S3ObjectStore {
    #[must_use]
    pub fn from_prefixed_env(prefix: &str) -> Self {
        let prefix_case = prefix.to_ascii_uppercase();
        let no_access_key_var = format!("{prefix}_AWS_ACCESS_KEY_ID is not set");
        let no_secret_key_var = format!("{prefix}_AWS_SECRET_ACCESS_KEY is not set");
        let no_region_var = format!("{prefix}_AWS_REGION is not set");
        let no_bucket_var = format!("{prefix}_AWS_BUCKET is not set");
        let no_endpoint_var = format!("{prefix}_AWS_ENDPOINT is not set");

        let region = std::env::var(format!("{prefix_case}_AWS_REGION")).expect(&no_region_var);
        //.unwrap_or("us-east-1".into());
        let access_key =
            std::env::var(format!("{prefix_case}_AWS_ACCESS_KEY_ID")).expect(&no_access_key_var);
        let secret_key = std::env::var(format!("{prefix_case}_AWS_SECRET_ACCESS_KEY"))
            .expect(&no_secret_key_var);
        let endpoint =
            std::env::var(format!("{prefix_case}_AWS_ENDPOINT")).expect(&no_endpoint_var);
        let bucket = std::env::var(format!("{prefix}_AWS_BUCKET")).expect(&no_bucket_var);

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

pub struct ExecutorWithObjectStore {
    executor: CoreExecutionService,
    object_store_type: ObjectStoreType,
}

#[derive(Debug, Clone)]
pub enum ObjectStoreType {
    Memory,
    File(String, PathBuf),     // + suffix
    S3(String, S3ObjectStore), // + suffix
}

// Display
impl fmt::Display for ObjectStoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => write!(f, "Memory"),
            Self::File(suffix, path) => write!(f, "File({}/{suffix})", path.display()),
            Self::S3(suffix, s3_object_store) => write!(
                f,
                "S3({}/{suffix})",
                s3_object_store
                    .s3_builder
                    .get_config_value(&AmazonS3ConfigKey::Bucket)
                    .unwrap_or_default()
            ),
        }
    }
}

impl ObjectStoreType {
    #[allow(clippy::as_conversions)]
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, Error> {
        match &self {
            Self::Memory => Ok(Arc::new(object_store::memory::InMemory::new())),
            Self::File(_, path, ..) => Ok(Arc::new(Self::object_store_at_path(path.as_path())?)),
            Self::S3(_, s3_object_store, ..) => s3_object_store
                .s3_builder
                .clone()
                .build()
                .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                .context(ObjectStoreSnafu),
        }
    }

    pub async fn db(&self) -> Result<Db, Error> {
        let db = match &self {
            Self::Memory => Db::memory().await,
            Self::File(suffix, ..) | Self::S3(suffix, ..) => Db::new(Arc::new(
                SlateDb::open_with_opts(
                    object_store::path::Path::from(suffix.clone()),
                    DbOptions::default(),
                    self.object_store()?,
                )
                .await
                .context(SlatedbSnafu)?,
            )),
        };

        Ok(db)
    }

    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn object_store_at_path(path: &Path) -> Result<Arc<dyn ObjectStore>, Error> {
        if !path.exists() || !path.is_dir() {
            fs::create_dir(path).unwrap();
        }
        LocalFileSystem::new_with_prefix(path)
            .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
            .context(ObjectStoreSnafu)
    }
}

pub async fn create_executor(
    object_store_type: ObjectStoreType,
    fs_volume_suffix: &str,
) -> Result<ExecutorWithObjectStore, Error> {
    let db = object_store_type.db().await?;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .expect("Failed to create execution service");

    // Create all kind of volumes to just use them in queries

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

    let mut user_data_dir = env::temp_dir();
    user_data_dir.push("store");
    user_data_dir.push(format!("user-volume-{fs_volume_suffix}"));
    let user_data_dir = user_data_dir.as_path();
    let _ = metastore
        .create_volume(
            &TEST_VOLUME_FILE.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_FILE.0.to_string(),
                core_metastore::VolumeType::File(FileVolume {
                    path: user_data_dir.display().to_string(),
                }),
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
        .create_session(TEST_SESSION_ID1.to_string())
        .await
        .expect("Failed to create session 1");

    execution_svc
        .create_session(TEST_SESSION_ID2.to_string())
        .await
        .expect("Failed to create session 2");

    Ok(ExecutorWithObjectStore {
        executor: execution_svc,
        object_store_type: object_store_type.clone(),
    })
}

// Every executor
pub async fn exec_parallel_test_plan(
    test_plan: Vec<ParallelTest>,
    volumes_databases_list: Vec<(&str, &str)>,
) -> Result<bool, Error> {
    let mut passed = true;

    for (volume_name, database_name) in &volumes_databases_list {
        for ParallelTest(tests) in &test_plan {
            // log context
            let object_store_types: HashSet<String> = tests
                .iter()
                .map(|test| test.executor.object_store_type.to_string())
                .collect();
            let sessions: HashSet<String> = tests
                .iter()
                .map(|test| test.session_id.to_string())
                .collect();
            eprintln!(
                "Parallel executors use object store types: {object_store_types:#?}, sessions: {sessions:#?}"
            );
            eprintln!("Volume: {volume_name}, Database: {database_name}");

            // create sqls array here sql ref to String won't survive in the loop below
            let tests_sqls = tests
                .iter()
                .map(|TestQuery { sqls, .. }| {
                    sqls.iter()
                        .map(|sql| prepare_statement(sql, volume_name, database_name))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let mut futures = Vec::new();
            for (idx, test) in tests.iter().enumerate() {
                match test.sqls.len() {
                    1 => {
                        // run in parallel (non blocking mode)
                        let sql = &tests_sqls[idx][0];
                        futures.push(test.executor.executor.query(
                            test.session_id,
                            sql,
                            QueryContext::default(),
                        ));
                    }
                    _ => {
                        for sql in &tests_sqls[idx] {
                            let res = test
                                .executor
                                .executor
                                .query(test.session_id, sql, QueryContext::default())
                                .await
                                .context(ExecutionSnafu { query: sql.clone() });
                            eprintln!("Exec: {sql}, res: {res:#?}");
                            res?;
                        }
                    }
                }
            }
            // we do not expect mixed multiple sqls and expectations running in parallel
            // run in parallel if one sql is specified
            let results = join_all(futures).await;
            if !results.is_empty() {
                let num_expected_ok = tests.iter().filter(|test| test.expected_res).count();
                let num_expected_err = tests.len() - num_expected_ok;

                let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
                let oks: Vec<_> = oks.into_iter().map(Result::unwrap).collect();
                let errs: Vec<_> = errs.into_iter().map(Result::unwrap_err).collect();

                let mut tests_sqls = tests_sqls.clone();
                tests_sqls.sort();
                tests_sqls.dedup();
                eprintln!("Run sqls in parallel: {tests_sqls:#?}");

                if oks.len() != num_expected_ok || errs.len() != num_expected_err {
                    eprintln!("ok_results: {oks:#?}, err_results: {errs:#?}");
                    eprintln!("FAILED\n");
                    passed = false;
                    break;
                }
                eprintln!("PASSED\n");
            }
        }
    }
    Ok(passed)
}

fn prepare_statement(raw_statement: &str, volume_name: &str, database_name: &str) -> String {
    raw_statement
        .replace("__VOLUME__", volume_name)
        .replace("__DATABASE__", database_name)
        .replace("__SCHEMA__", TEST_SCHEMA_NAME)
}
