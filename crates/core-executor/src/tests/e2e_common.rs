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
use core_metastore::{FileVolume, S3Volume, S3TablesVolume};
use core_utils::Db;
use futures::future::join_all;
use object_store::ObjectStore;
use object_store::{
    aws::AmazonS3Builder, aws::AmazonS3ConfigKey, aws::S3ConditionalPut, local::LocalFileSystem,
};
use slatedb::{Db as SlateDb, config::DbOptions};
use snafu::ResultExt;
use snafu::{Location, Snafu};
use std::env::{self, VarError};
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

// Set envs, and add to .env

// # Env vars for s3 object store
// AWS_ACCESS_KEY_ID=
// AWS_SECRET_ACCESS_KEY=
// AWS_REGION=us-east-1
// AWS_BUCKET=tables-data
// AWS_ENDPOINT=http://localhost:9000
// AWS_ALLOW_HTTP=true

const E2E_S3VOLUME_PREFIX: &str = "E2E_S3VOLUME";
// Env vars for S3Volume:
// E2E_S3VOLUME_AWS_ACCESS_KEY_ID=
// E2E_S3VOLUME_AWS_SECRET_ACCESS_KEY=
// E2E_S3VOLUME_AWS_REGION=us-east-1
// E2E_S3VOLUME_AWS_BUCKET=e2e-store
// E2E_S3VOLUME_AWS_ENDPOINT=http://localhost:9000

const E2E_S3TABLESVOLUME_PREFIX: &str = "E2E_S3TABLESVOLUME";
// Env vars for S3TablesVolume:
// E2E_S3TABLESVOLUME_AWS_ACCESS_KEY_ID=
// E2E_S3TABLESVOLUME_AWS_SECRET_ACCESS_KEY=
// E2E_S3TABLESVOLUME_AWS_ARN=arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket
// E2E_S3TABLESVOLUME_AWS_ENDPOINT=http://localhost:9000

pub const TEST_SESSION_ID1: &str = "test_session_id1";
pub const TEST_SESSION_ID2: &str = "test_session_id2";

pub const TEST_VOLUME_MEMORY: (&str, &str) = ("volume_memory", "database_in_memory");
pub const TEST_VOLUME_FILE: (&str, &str) = ("volume_file", "database_in_file");
pub const TEST_VOLUME_S3: (&str, &str) = ("volume_s3", "database_in_s3");
pub const TEST_VOLUME_S3TABLES: (&str, &str) = ("volume_s3tables", "database_in_s3tables");

pub const TEST_DATABASE_NAME: &str = "embucket";
pub const TEST_SCHEMA_NAME: &str = "public";

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
    Execution {
        query: String,
        source: crate::Error,
        #[snafu(implicit)]
        location: Location,
    },
    S3VolumeConfig {
        source: VarError,
        #[snafu(implicit)]
        location: Location,
    },
    S3TablesVolumeConfig {
        source: VarError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[must_use]
pub fn test_suffix() -> String {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis())
        .to_string()
}

#[must_use]
pub fn s3_volume() -> Result<S3Volume, Error> {
    let prefix = E2E_S3VOLUME_PREFIX.to_ascii_uppercase();

    let region = std::env::var(format!("{prefix}_AWS_REGION"))
        .context(S3VolumeConfigSnafu)?;
    let access_key =
        std::env::var(format!("{prefix}_AWS_ACCESS_KEY_ID"))
        .context(S3VolumeConfigSnafu)?;
    let secret_key = std::env::var(format!("{prefix}_AWS_SECRET_ACCESS_KEY"))
        .context(S3VolumeConfigSnafu)?;
    let endpoint =
        std::env::var(format!("{prefix}_AWS_ENDPOINT"))
        .context(S3VolumeConfigSnafu)?;
    let bucket = std::env::var(format!("{prefix}_AWS_BUCKET"))
        .context(S3VolumeConfigSnafu)?;

    Ok(S3Volume {
        region: Some(region),
        bucket: Some(bucket),
        endpoint: Some(endpoint),
        credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: access_key,
            aws_secret_access_key: secret_key,
        })),
    })
}


#[must_use]
pub fn s3_tables_volume(database: &str) -> Result<S3TablesVolume, Error> {
    let prefix = E2E_S3TABLESVOLUME_PREFIX.to_ascii_uppercase();

    let access_key =
        std::env::var(format!("{prefix}_AWS_ACCESS_KEY_ID"))
        .context(S3TablesVolumeConfigSnafu)?;
    let secret_key = std::env::var(format!("{prefix}_AWS_SECRET_ACCESS_KEY"))
        .context(S3TablesVolumeConfigSnafu)?;
    let arn = std::env::var(format!("{prefix}_AWS_ARN"))
        .context(S3TablesVolumeConfigSnafu)?;
    let endpoint: Option<String> = std::env::var(format!("{prefix}_AWS_ENDPOINT"))
        .map(|v| Some(v))
        .unwrap_or(None);

    Ok(S3TablesVolume {
        endpoint,
        credentials: AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: access_key,
            aws_secret_access_key: secret_key,
        }),
        database: database.to_string(),
        arn,
    })
}

pub type TestPlan = Vec<ParallelTest>;

pub struct ParallelTest(pub Vec<TestQuery>);

pub struct TestQuery {
    pub sqls: Vec<&'static str>,
    pub executor: Arc<ExecutorWithObjectStore>,
    pub session_id: &'static str,
    pub expected_res: bool,
}

pub struct S3TableStore {
    pub s3_builder: AmazonS3Builder,
}

#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    pub s3_builder: AmazonS3Builder,
}
impl S3ObjectStore {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            s3_builder: AmazonS3Builder::from_env()
                .with_conditional_put(S3ConditionalPut::ETagMatch)
        }
    }
}

pub struct ExecutorWithObjectStore {
    pub executor: CoreExecutionService,
    pub object_store_type: ObjectStoreType,
    pub alias: String,
}

impl ExecutorWithObjectStore {
    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = alias;
        self
    }
}

#[derive(Debug, Clone)]
pub enum ObjectStoreType {
    Memory,
    File(String, PathBuf),    // + suffix
    S3(String, S3ObjectStore) // + suffix
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
            )
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
    alias: &str,
) -> Result<ExecutorWithObjectStore, Error> {
    let db = object_store_type.db().await?;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    );

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

    if let Ok(s3_volume) = s3_volume() {
        let _ = metastore
        .create_volume(
            &TEST_VOLUME_S3.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_S3.0.to_string(),
                core_metastore::VolumeType::S3(s3_volume),
            ),
        )
        .await;        
    }

    if let Ok(s3_tables_volume) = s3_tables_volume(TEST_VOLUME_S3TABLES.1) {
        let _ = metastore
        .create_volume(
            &TEST_VOLUME_S3TABLES.0.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_S3TABLES.0.to_string(),
                core_metastore::VolumeType::S3Tables(s3_tables_volume),
            ),
        )
        .await;
    }

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
        alias: alias.to_string(),
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
            // create sqls array here sql ref to String won't survive in the loop below
            let tests_sqls = tests
                .iter()
                .map(|TestQuery { sqls, .. }| {
                    sqls.iter()
                        .map(|sql| prepare_statement(sql, volume_name, database_name))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            // run batch sqls if any
            for (idx, test) in tests.iter().enumerate() {
                // get slice of all items except last
                let items = &tests_sqls[idx];
                let sync_items = &items[..items.len().saturating_sub(1)];

                // run synchronously all the queries except of last
                // these items are expected to pass
                for sql in sync_items {
                    let res = test
                        .executor
                        .executor
                        .query(test.session_id, sql, QueryContext::default())
                        .await
                        .context(ExecutionSnafu { query: sql.clone() });
                    let ExecutorWithObjectStore { alias, object_store_type, .. } = test.executor.as_ref();
                    eprintln!("Exec synchronously with executor [{alias}], on object store: {object_store_type}, session: {}", test.session_id);
                    eprintln!("sql: {sql}\nres: {res:#?}");
                    res?;
                }
            }

            let mut parallel_runs = Vec::new();

            // run sqls concurrently
            let mut futures = Vec::new();
            for (idx, test) in tests.iter().enumerate() {
                // get slice of all items except last
                let items = &tests_sqls[idx];

                // run last item from every TestQuery in (non blocking mode)
                if let Some(sql) = items.last() {
                    futures.push(test.executor.executor.query(
                        test.session_id,
                        sql,
                        QueryContext::default(),
                    ));
                    parallel_runs.push((sql, test));
                }
            }

            let results = join_all(futures).await;

            for (idx, (sql, test)) in parallel_runs.iter().enumerate() {
                let TestQuery { expected_res, session_id, .. } = test;
                let res = &results[idx];
                let test_num = idx + 1;
                let parallel_runs = parallel_runs.len();
                let ExecutorWithObjectStore { alias, object_store_type, .. } = test.executor.as_ref();
                eprintln!("Exec concurrently with executor [{alias}], on object store: {object_store_type}, session: {session_id}");
                eprintln!("sql {test_num}/{parallel_runs}: {sql}\nexpected_res: {expected_res}, res: {res:#?}");
                if expected_res != &res.is_ok() {
                    eprintln!("FAILED\n");
                    passed = false;
                } else {
                    eprintln!("PASSED\n");    
                }
            }

            if !passed {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn prepare_statement(raw_statement: &str, volume_name: &str, database_name: &str) -> String {
    raw_statement
        .replace("__VOLUME__", volume_name)
        .replace("__DATABASE__", database_name)
        .replace("__SCHEMA__", TEST_SCHEMA_NAME)
}
