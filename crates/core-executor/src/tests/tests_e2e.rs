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
use dotenv::dotenv;
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
    Utc::now().timestamp_nanos_opt().unwrap_or_else(
        ||Utc::now().timestamp_millis()
    ).to_string()
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

pub struct ParallelTest(Vec<TestQuery>);

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
async fn exec_parallel_test_plan(
    test_plan: Vec<ParallelTest>,
    volumes_databases_list: Vec<(&str, &str)>,
) -> Result<bool, Error> {
    let mut passed = true;

    for (volume_name, database_name) in &volumes_databases_list {
        for ParallelTest(tests) in &test_plan {
            // error log context
            let object_store_types: HashSet<String> = tests
                .iter()
                .map(|test| test.executor.object_store_type.to_string())
                .collect();
            eprintln!("Parallel executors object store types are: {object_store_types:#?}");

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
                    eprintln!("Current VOLUME: {volume_name}, DATABASE: {database_name}");
                    eprintln!("Parallel executors object store types are: {object_store_types:#?}");
                    eprintln!("FAILED\n");
                    passed = false;
                    break;
                }
                eprintln!("Current VOLUME: {volume_name}, DATABASE: {database_name}");
                eprintln!("Parallel executors object store types are: {object_store_types:#?}");
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

pub fn template_single_executor_two_sessions_different_tables_inserts(
    executor: Arc<ExecutorWithObjectStore>,
) -> Vec<ParallelTest> {
    // Running single Embucket (all volumes), two sessions, writes to different tables
    vec![
        ParallelTest(vec![TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            ],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
        }]),
        ParallelTest(vec![
            TestQuery {
                sqls: vec![INSERT_INTO_ALL_SNOWFLAKE_TYPES],
                executor: executor.clone(),
                session_id: TEST_SESSION_ID1,
                expected_res: true,
            },
            TestQuery {
                sqls: vec![
                    "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux');",
                ],
                executor,
                session_id: TEST_SESSION_ID2,
                expected_res: true,
            },
        ]),
    ]
}

pub fn template_two_unrelated_executors_inserts_into_different_tables(
    executor1: Arc<ExecutorWithObjectStore>,
    executor2: Arc<ExecutorWithObjectStore>,
) -> Vec<ParallelTest> {
    vec![
        ParallelTest(vec![
            TestQuery {
                sqls: vec![
                    "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                    "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                    CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
                    "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                ],
                executor: executor1.clone(),
                session_id: TEST_SESSION_ID1,
                expected_res: true,
            },
            TestQuery {
                sqls: vec![
                    "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                    "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                    CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
                    "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                ],
                executor: executor2.clone(),
                session_id: TEST_SESSION_ID1,
                expected_res: true,
            },
        ]),
        ParallelTest(vec![
            TestQuery {
                sqls: vec![INSERT_INTO_ALL_SNOWFLAKE_TYPES],
                executor: executor1,
                session_id: TEST_SESSION_ID1,
                expected_res: true,
            },
            TestQuery {
                sqls: vec![
                    "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux');",
                ],
                executor: executor2,
                session_id: TEST_SESSION_ID1,
                expected_res: true,
            },
        ]),
    ]
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_two_unrelated_executors() -> Result<(), Error> {
    dotenv().ok();

    let test_suffix1 = test_suffix();
    let test_suffix2 = test_suffix();

    let file_exec1 = create_executor(
        ObjectStoreType::File(test_suffix1.clone(), env::temp_dir().join("store")),
        &test_suffix1,
    )
    .await?;

    let file_exec2 = create_executor(
        ObjectStoreType::File(test_suffix2.clone(), env::temp_dir().join("store")),
        &test_suffix2,
    )
    .await?;

    let test_plan = template_two_unrelated_executors_inserts_into_different_tables(
        Arc::new(file_exec1),
        Arc::new(file_exec2),
    );

    assert!(
        exec_parallel_test_plan(
            test_plan,
            vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_file_store_single_executor_two_sessions_different_tables_inserts()
-> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    // let storages = vec![
    //     ObjectStoreType::Memory,
    //     ObjectStoreType::File(test_suffix.clone()),
    //     ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
    // ];

    let file_exec = create_executor(
        ObjectStoreType::File(test_suffix.clone(), env::temp_dir().join("store")),
        &test_suffix,
    )
    .await?;

    let test_plan =
        template_single_executor_two_sessions_different_tables_inserts(Arc::new(file_exec));

    assert!(
        exec_parallel_test_plan(
            test_plan,
            vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_memory_store_single_executor_two_sessions_different_tables_inserts()
-> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    // let storages = vec![
    //     ObjectStoreType::Memory,
    //     ObjectStoreType::File(test_suffix.clone()),
    //     ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
    // ];

    let memory_exec = create_executor(ObjectStoreType::Memory, &test_suffix).await?;

    let test_plan =
        template_single_executor_two_sessions_different_tables_inserts(Arc::new(memory_exec));

    assert!(
        exec_parallel_test_plan(
            test_plan,
            vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_s3_store_single_executor_two_sessions_different_tables_inserts()
-> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    // let storages = vec![
    //     ObjectStoreType::Memory,
    //     ObjectStoreType::File(test_suffix.clone()),
    //     ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
    // ];

    let s3_exec = create_executor(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_prefixed_env("E2E_STORE"),
        ),
        &test_suffix,
    )
    .await?;

    let test_plan =
        template_single_executor_two_sessions_different_tables_inserts(Arc::new(s3_exec));

    assert!(
        exec_parallel_test_plan(
            test_plan,
            vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
        )
        .await?
    );
    Ok(())
}

// #[tokio::test]
// #[ignore = "e2e test"]
// #[allow(clippy::expect_used, clippy::too_many_lines)]
// async fn e2e_test_same_file_object_store_two_executors_create_table() -> Result<(), Error> {
//     dotenv().ok();

//     let test_suffix = test_suffix();

//     // All available oject store types:
//     // ObjectStoreType::Memory,
//     // ObjectStoreType::File(test_suffix.clone()),
//     // ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),

//     let object_store_file = ObjectStoreType::File(env::temp_dir().join("store"));
//     let file_exec1 = create_executor(&object_store_file, &test_suffix).await?;
//     let file_exec2 = create_executor(&object_store_file, &test_suffix).await?;

//     let mut test_plan: TestPlan<'_> = indexmap::IndexMap::new();
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true)],
//     );
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE SCHEMA __DATABASE__.__SCHEMA__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true)],
//     );

//     // test statement
//     test_plan.insert(
//         CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
//         vec![(&file_exec1, TEST_SESSION_ID1, true), (&file_exec2, TEST_SESSION_ID1, true)],
//     );

//     assert!(
//         exec_parallel_test_plan(
//             test_plan,
//             vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
//         )
//         .await?
//     );
//     Ok(())
// }

// #[tokio::test]
// #[ignore = "e2e test"]
// #[allow(clippy::expect_used, clippy::too_many_lines)]
// async fn e2e_test_same_file_object_store_two_executors_fail() -> Result<(), Error> {
//     dotenv().ok();

//     let test_suffix = test_suffix();

//     let object_store_file = ObjectStoreType::File(env::temp_dir().join("store"));

//     let file_exec1 = create_executor(&object_store_file, &test_suffix).await?;
//     let file_exec2 = create_executor(&object_store_file, &test_suffix).await?;

//     let mut test_plan: TestPlan<'_> = indexmap::IndexMap::new();
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true), (&file_exec2, TEST_SESSION_ID1, false)],
//     );
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE SCHEMA __DATABASE__.__SCHEMA__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true), (&file_exec2, TEST_SESSION_ID1, false)],
//     );

//     assert!(
//         exec_parallel_test_plan(
//             test_plan,
//             vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
//         )
//         .await
//         .is_err()
//     );
//     Ok(())
// }

// #[tokio::test]
// #[ignore = "e2e test"]
// #[allow(clippy::expect_used, clippy::too_many_lines)]
// async fn e2e_test_same_file_object_store_two_executors_inserts() -> Result<(), Error> {
//     dotenv().ok();

//     let test_suffix = test_suffix();

//     // let storages = vec![
//     //     ObjectStoreType::Memory,
//     //     ObjectStoreType::File(test_suffix.clone()),
//     //     ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
//     // ];

//     let object_store_file = ObjectStoreType::File(env::temp_dir().join("store"));
//     let file_exec1 = create_executor(&object_store_file, &test_suffix).await?;
//     let file_exec2 = create_executor(&object_store_file, &test_suffix).await?;

//     let mut test_plan: TestPlan<'_> = indexmap::IndexMap::new();
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true)],
//     );
//     // prerequisite statement
//     test_plan.insert(
//         "CREATE SCHEMA __DATABASE__.__SCHEMA__",
//         vec![(&file_exec1, TEST_SESSION_ID1, true)],
//     );
//     // prerequisite statement
//     test_plan.insert(
//         CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
//         vec![(&file_exec1, TEST_SESSION_ID1, true)],
//     );

//     // Test INSERTs
//     test_plan.insert(
//         INSERT_INTO_ALL_SNOWFLAKE_TYPES,
//         vec![(&file_exec1, TEST_SESSION_ID1, true), (&file_exec2, TEST_SESSION_ID1, false)],
//     );
//     test_plan.insert(
//         INSERT_INTO_ALL_SNOWFLAKE_TYPES,
//         vec![(&file_exec1, TEST_SESSION_ID1, true), (&file_exec2, TEST_SESSION_ID1, false)],
//     );

//     assert!(
//         exec_parallel_test_plan(
//             test_plan,
//             vec![TEST_VOLUME_MEMORY, TEST_VOLUME_FILE, TEST_VOLUME_S3]
//         )
//         .await?
//     );
//     Ok(())
// }

const CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES: &str =
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
)";
const INSERT_INTO_ALL_SNOWFLAKE_TYPES: &str =
    "INSERT INTO __DATABASE__.__SCHEMA__.all_snowflake_types VALUES (
 -- Numeric Types
    1, 1.1, 1.1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    -- String Types
    -- col_character CHARACTER(10),
    'a', 'b', 'c', 'd',
    -- Boolean
    false,
    -- Date & Time Types
    '2022-01-01', 
    -- col_time TIME,
    '2022-01-01 00:00:00', 
    '2022-01-01 00:00:00', 
    '2022-01-01 00:00:00', 
    '2022-01-01 00:00:00', 
    '2022-01-01 00:00:00',
    -- Semi-structured
    '{\"a\": 1, \"b\": 2}',
    '{\"a\": 1, \"b\": 2}',
    '{\"a\": 1, \"b\": 2}',
    -- Binary
    'a', 'b'
    -- Geography (optional feature)
    -- col_geography GEOGRAPHY
)";

// const PREREQUISITE_STATEMENTS: [&str; 2] = [
//     "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
//     "CREATE SCHEMA __DATABASE__.__SCHEMA__",
// ];
// const TEST_STATEMENTS: [&str; 5] = [
//     ,
//     "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
//     // Seems alter table is not working
//     // "ALTER TABLE __DATABASE__.__SCHEMA__.hello ADD COLUMN c5 VARCHAR",
//     "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
//     (100, 'Alice', 'foo'),
//     (200, 'Bob', 'bar'),
//     (300, 'Charlie', 'baz'),
//     (400, 'Diana', 'qux'),
//     (500, 'Eve', 'quux');",
//     "DELETE FROM __DATABASE__.__SCHEMA__.hello WHERE c5 LIKE '%ux%'",
//     "ALTER TABLE __DATABASE__.__SCHEMA__.hello DROP COLUMN c5",
// ];
