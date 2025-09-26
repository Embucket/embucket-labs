#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
use super::e2e_common::TestAwsSdkSnafu;
use super::e2e_toxiproxy::{
    create_toxic_conn_limit, create_toxiproxy, delete_toxic_conn_limit, delete_toxiproxy,
};
use crate::error_code::ErrorCode;
use crate::service::ExecutionService;
use crate::tests::e2e::e2e_common::{
    AWS_OBJECT_STORE_PREFIX, E2E_S3TABLESVOLUME_PREFIX, E2E_S3VOLUME_PREFIX, Error,
    MINIO_OBJECT_STORE_PREFIX, ObjectStoreType, ParallelTest, S3ObjectStore, TEST_SESSION_ID1,
    TEST_SESSION_ID2, TEST_SESSION_ID3, TestQuery, TestQueryCallback, TestVolumeType, VolumeConfig,
    copy_env_to_new_prefix, create_executor, create_executor_with_early_volumes_creation,
    create_s3tables_client, exec_parallel_test_plan, s3_tables_volume, test_suffix,
};
use crate::tests::e2e::e2e_s3tables_aws::{
    delete_s3tables_bucket_table, delete_s3tables_bucket_table_policy,
    get_s3tables_tables_arns_map, set_s3table_bucket_table_policy, set_table_bucket_policy,
};
use dotenv::dotenv;
use snafu::ResultExt;
use std::env;
use std::sync::Arc;
use std::time::Duration;

const S3TABLES_BUCKET_DENY_READ_WRITE_POLICY_DATA: &str = r#"
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "DenyReadWriteS3TablesAccess",
          "Effect": "Deny",
          "Principal": "*",
          "Action": [
            "s3tables:PutTableData",
            "s3tables:GetTableData"
          ],
          "Resource": "__BUCKET_ARN__/*"
        }  
      ]
    }
    "#;

const S3TABLES_TABLE_DENY_WRITE_POLICY_DATA: &str = r#"
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "DenyWriteS3TablesAccess",
        "Effect": "Deny",
        "Principal": "*",
        "Action": [
          "s3tables:PutTableData"
        ],
        "Resource": "__ARN__DENY_WRITE_TABLE_UUID__"
      }
    ]
}
"#;

const S3TABLES_TABLE_DENY_READWRITE_POLICY_DATA: &str = r#"
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "DenyReadWriteS3TablesAccess",
        "Effect": "Deny",
        "Principal": "*",
        "Action": [
          "s3tables:PutTableData",
          "s3tables:GetTableData"
        ],
        "Resource": "__ARN__DENY_READWRITE_TABLE_UUID__"
      }
    ]
}
"#;

async fn template_test_two_executors_file_object_store_one_writer_fences_another(
    volumes: &[TestVolumeType],
    delay: Option<Duration>,
) -> Result<(), Error> {
    let test_suffix = test_suffix();

    let object_store_file =
        ObjectStoreType::File(test_suffix.clone(), env::temp_dir().join("store"));

    let file_exec1 = create_executor(object_store_file.clone(), "#1").await?;
    let file_exec1 = Arc::new(file_exec1);

    // create data using first executor
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
        ],
        executor: file_exec1.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes).await?);

    // create 2nd executor on the same object store
    let file_exec2 = create_executor(object_store_file, "#2").await?;
    let file_exec2 = Arc::new(file_exec2);

    // write data using 2nd executor
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
            // INSERT instead of SELECT to avoid possible false positive due to possible caching behaviour
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
        ],
        executor: file_exec2.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes).await?);

    // give delay for sync job to run
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await; // Ensure the executor is created after the previous delay
    }

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            // After being fenced:
            sqls: vec![
                // first executor still successfully reads data
                "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
            ],
            executor: file_exec1.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            // After being fenced:
            sqls: vec![
                // first executor fails to write
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
            (100, 'Alice', 'foo')",
            ],
            executor: file_exec1.clone(),
            session_id: TEST_SESSION_ID2,
            expected_res: false,
            err_callback: None,
        },
        TestQuery {
            // After being fenced:
            sqls: vec![
                // first executor still successfully reads data
                "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
            ],
            executor: file_exec1,
            session_id: TEST_SESSION_ID3,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec![
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                (100, 'Alice', 'foo')",
            ],
            executor: file_exec2,
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
    ])];
    assert!(exec_parallel_test_plan(test_plan, volumes).await?);

    Ok(())
}

async fn template_test_s3_store_single_executor_with_old_and_freshly_created_sessions(
    volumes: &[TestVolumeType],
) -> Result<(), Error> {
    let executor = create_executor(
        ObjectStoreType::S3(
            test_suffix(),
            S3ObjectStore::from_env(MINIO_OBJECT_STORE_PREFIX)?,
        ),
        "s3_exec",
    )
    .await?;
    let executor = Arc::new(executor);

    let prerequisite_test = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
            "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(prerequisite_test, volumes).await?);

    // Here use freshly created sessions instead of precreated
    let newly_created_session = "newly_created_session";
    executor
        .executor
        .create_session(newly_created_session)
        .await
        .expect("Failed to create newly_created_session");

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            sqls: vec![INSERT_INTO_ALL_SNOWFLAKE_TYPES],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec![
                // test if database and schema created in other sessions can be resolved in this session
                "CREATE TABLE __DATABASE__.__SCHEMA__.yyy(test number)",
                // test if table created in other sessions can be resolved in this session
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                    (100, 'Alice', 'foo'),
                    (200, 'Bob', 'bar'),
                    (300, 'Charlie', 'baz'),
                    (400, 'Diana', 'qux'),
                    (500, 'Eve', 'quux');",
            ],
            executor,
            session_id: newly_created_session,
            expected_res: true,
            err_callback: None,
        },
    ])];

    assert!(exec_parallel_test_plan(test_plan, volumes).await?);
    Ok(())
}

fn toxiproxy_name_and_payload(some_id: usize, port: usize) -> (String, String) {
    let minio_proxy_name = format!("minio-proxy-{some_id}");
    let toxic_minio_proxy_payload = format!(
        r#"{{
        "name": "{minio_proxy_name}",
        "listen": "0.0.0.0:{port}",
        "upstream": "localhost:9000"
    }}"#
    );
    (minio_proxy_name, toxic_minio_proxy_payload)
}

async fn template_s3_connections_test2(
    metastore_env_prefix: String,
    volume_env_prefix: String,
    create_executor_expected_res: bool,
    sql_expected_res: bool,
    err_callback: Option<Box<dyn TestQueryCallback>>,
) -> Result<bool, Error> {
    let volume_env_prefix: &'static str = Box::leak(volume_env_prefix.into_boxed_str());

    let test_suffix = test_suffix();
    let database_name: &'static str = Box::leak("database".repeat(2000).into_boxed_str());
    let executor = create_executor_with_early_volumes_creation(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_env(&metastore_env_prefix)?,
        ),
        "s3 executor",
        vec![VolumeConfig {
            prefix: Some(volume_env_prefix),
            volume_type: TestVolumeType::S3,
            volume: "s3_volume_with_toxic",
            database: database_name,
            schema: "schema", // schema_name,
        }],
    )
    .await;
    assert_eq!(create_executor_expected_res, executor.is_ok());

    let executor = Arc::new(executor?);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            // "CREATE SCHEMA __DATABASE__.__SCHEMA__",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: sql_expected_res,
        err_callback,
    }])];
    exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await
}

async fn template_s3_connections_test3(
    metastore_env_prefix: String,
    volume_env_prefix: String,
    create_executor_expected_res: bool,
    sql_expected_res: bool,
    err_callback: Option<Box<dyn TestQueryCallback>>,
) -> Result<bool, Error> {
    let volume_env_prefix: &'static str = Box::leak(volume_env_prefix.into_boxed_str());

    let test_suffix = test_suffix();
    let schema_name: &'static str = Box::leak("schema".repeat(3000).into_boxed_str());
    let executor = create_executor_with_early_volumes_creation(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_env(&metastore_env_prefix)?,
        ),
        "s3 executor",
        vec![VolumeConfig {
            prefix: Some(volume_env_prefix),
            volume_type: TestVolumeType::S3,
            volume: "s3_volume_with_toxic",
            database: "database_s3",
            schema: schema_name,
        }],
    )
    .await;
    assert_eq!(create_executor_expected_res, executor.is_ok());

    let executor = Arc::new(executor?);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: sql_expected_res,
        err_callback,
    }])];
    exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await
}

async fn template_s3_connections_test(
    metastore_env_prefix: String,
    volume_env_prefix: String,
    create_executor_expected_res: bool,
    sql_expected_res: bool,
    err_callback: Option<Box<dyn TestQueryCallback>>,
) -> Result<bool, Error> {
    let volume_env_prefix: &'static str = Box::leak(volume_env_prefix.into_boxed_str());

    let test_suffix = test_suffix();
    // create unique db name as volume will use this as a path for the data
    let database_name: &'static str = Box::leak(format!("db_{test_suffix}").into_boxed_str());
    let executor = create_executor_with_early_volumes_creation(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_env(&metastore_env_prefix)?,
        ),
        "s3 executor",
        vec![VolumeConfig {
            prefix: Some(volume_env_prefix),
            volume_type: TestVolumeType::S3,
            volume: "s3_volume_with_toxic",
            database: database_name,
            schema: "schema",
        }],
    )
    .await;
    assert_eq!(create_executor_expected_res, executor.is_ok());

    let executor = Arc::new(executor?);

    let huge_schema_to_trigger_connection_limit = format!(
        "CREATE TABLE __DATABASE__.__SCHEMA__.test({});",
        (0..200)
            .map(|i| format!("amount{i} number, name{i} string, c5{i} VARCHAR"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let huge_schema_to_trigger_connection_limit: &'static str =
        Box::leak(huge_schema_to_trigger_connection_limit.into_boxed_str());

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            huge_schema_to_trigger_connection_limit,
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: sql_expected_res,
        err_callback,
    }])];
    exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_memory_store_s3_tables_volumes() -> Result<(), Error> {
    const TEST_SCHEMA_NAME: &str = "test1";

    eprintln!(
        "This test creates volumes ahead of the executor as it is required by s3tables.\
    Removes previously set restrictive policies, then creates test data and assigns those \
    restrictive policies to table_ro, table_no_access and tests read, write, no access as expected."
    );
    dotenv().ok();

    // this test uses separate tables policies so
    // parallel tests can operate on the same bucket with other tables set

    let client = create_s3tables_client(E2E_S3TABLESVOLUME_PREFIX).await?;
    let bucket_arn = s3_tables_volume("", E2E_S3TABLESVOLUME_PREFIX)?.arn;

    let _ = delete_s3tables_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_ro".to_string(),
    )
    .await
    .context(TestAwsSdkSnafu);

    let _ = delete_s3tables_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_no_access".to_string(),
    )
    .await
    .context(TestAwsSdkSnafu);

    // Currently embucket can only read database from s3tables volume when created before executor
    let exec = create_executor_with_early_volumes_creation(
        ObjectStoreType::Memory(test_suffix()),
        "memory_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_S3TABLESVOLUME_PREFIX),
            volume_type: TestVolumeType::S3Tables,
            volume: "volume_s3tables",
            database: "database_in_s3tables",
            schema: TEST_SCHEMA_NAME,
        }],
    )
    .await?;
    let exec = Arc::new(exec);

    // create tables & assign separate read, write policies
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            // "SHOW DATABASES",
            // "SHOW TABLES IN __DATABASE__.__SCHEMA__",
            "CREATE DATABASE IF NOT EXISTS __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__",
            "CREATE TABLE IF NOT EXISTS __DATABASE__.__SCHEMA__.table_ro(amount number, name string, c5 VARCHAR)",
            "CREATE TABLE IF NOT EXISTS __DATABASE__.__SCHEMA__.table_no_access(amount number, name string, c5 VARCHAR)",
            "INSERT INTO __DATABASE__.__SCHEMA__.table_ro (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo')",
            "INSERT INTO __DATABASE__.__SCHEMA__.table_no_access (amount, name, c5) VALUES 
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz')",
        ],
        executor: exec.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3Tables]).await?);

    // get tables arns to assign policies
    let tables_arns = get_s3tables_tables_arns_map(&client, bucket_arn.clone())
        .await
        .context(TestAwsSdkSnafu)?;

    set_s3table_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_ro".to_string(),
        S3TABLES_TABLE_DENY_WRITE_POLICY_DATA.replace(
            "__ARN__DENY_WRITE_TABLE_UUID__",
            &tables_arns[&format!("{TEST_SCHEMA_NAME}.table_ro")].clone(),
        ),
    )
    .await
    .context(TestAwsSdkSnafu)?;

    set_s3table_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_no_access".to_string(),
        S3TABLES_TABLE_DENY_READWRITE_POLICY_DATA.replace(
            "__ARN__DENY_READWRITE_TABLE_UUID__",
            &tables_arns[&format!("{TEST_SCHEMA_NAME}.table_no_access")].clone(),
        ),
    )
    .await
    .context(TestAwsSdkSnafu)?;

    let test_plan = vec![
        ParallelTest(vec![TestQuery {
            sqls: vec![
                // allowed operarions after permissions set
                "SELECT * FROM __DATABASE__.__SCHEMA__.table_ro",
            ],
            executor: exec.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        }]),
        ParallelTest(vec![
            TestQuery {
                sqls: vec![
                    // not allowed operarions after permissions set
                    "INSERT INTO __DATABASE__.__SCHEMA__.table_ro (amount, name, c5) VALUES 
                            (400, 'Diana', 'qux')",
                ],
                executor: exec.clone(),
                session_id: TEST_SESSION_ID1,
                expected_res: false,
                err_callback: None,
            },
            TestQuery {
                sqls: vec![
                    // not allowed operarions after permissions set
                    "SELECT * FROM __DATABASE__.__SCHEMA__.table_no_access",
                ],
                executor: exec,
                session_id: TEST_SESSION_ID2,
                expected_res: false,
                err_callback: None,
            },
        ]),
    ];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3Tables]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_memory_store_s3_tables_volumes_not_permitted_select_returns_data_behaviour()
-> Result<(), Error> {
    const TEST_SCHEMA_NAME: &str = "test_non_permitted_selects";

    eprintln!(
        "This test creates volumes ahead of the executor as expected by s3tables volumes. \
    Then it creates a table, adds some data and assigns read/write deny policies to it. \
    Then select query returns data but shouldn't. This test passes now but left as a regression test."
    );
    dotenv().ok();

    // this test uses separate tables policies so
    // parallel tests can operate on the same bucket with other tables set

    let client = create_s3tables_client(E2E_S3TABLESVOLUME_PREFIX).await?;
    let bucket_arn = s3_tables_volume("", E2E_S3TABLESVOLUME_PREFIX)?.arn;

    let _ = delete_s3tables_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_no_access".to_string(),
    )
    .await
    .context(TestAwsSdkSnafu);

    // Currently embucket can only read database from s3tables volume when created before executor
    let exec = create_executor_with_early_volumes_creation(
        ObjectStoreType::Memory(test_suffix()),
        "memory_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_S3TABLESVOLUME_PREFIX),
            volume_type: TestVolumeType::S3Tables,
            volume: "volume_s3tables",
            database: "database_in_s3tables_no_selects",
            schema: TEST_SCHEMA_NAME,
        }],
    )
    .await?;
    let exec = Arc::new(exec);

    // create tables & assign separate read, write policies
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE IF NOT EXISTS __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__",
            "CREATE TABLE IF NOT EXISTS __DATABASE__.__SCHEMA__.table_no_access(amount number, name string, c5 VARCHAR)",
            "INSERT INTO __DATABASE__.__SCHEMA__.table_no_access (amount, name, c5) VALUES 
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz')",
            "SELECT count(*) FROM __DATABASE__.__SCHEMA__.table_no_access",
        ],
        executor: exec.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3Tables]).await?);

    // get tables arns to assign policies
    let tables_arns = get_s3tables_tables_arns_map(&client, bucket_arn.clone())
        .await
        .context(TestAwsSdkSnafu)?;

    eprintln!("tables arns: {tables_arns:?}");
    set_s3table_bucket_table_policy(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_no_access".to_string(),
        S3TABLES_TABLE_DENY_READWRITE_POLICY_DATA.replace(
            "__ARN__DENY_READWRITE_TABLE_UUID__",
            &tables_arns[&format!("{TEST_SCHEMA_NAME}.table_no_access")].clone(),
        ),
    )
    .await
    .context(TestAwsSdkSnafu)?;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            // not allowed operarions after permissions set
            "SELECT count(*) FROM __DATABASE__.__SCHEMA__.table_no_access",
        ],
        executor: exec,
        session_id: TEST_SESSION_ID2,
        expected_res: false,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3Tables]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_s3_tables_volumes_deny_rw_create_table_inconsistency_bug()
-> Result<(), Error> {
    const TEST_SCHEMA_NAME: &str = "test_create_table_inconsistency_bug";
    const E2E_S3TABLESVOLUME2_PREFIX: &str = "E2E_S3TABLESVOLUME2_";

    eprintln!(
        "This test assigns deny policy to s3tables bucket and runs create table sql, which fails as expected, \
    but creates table artifact in bucket. So subsequent run of executor/Embucket fails. Issue #1422"
    );
    dotenv().ok();

    copy_env_to_new_prefix(
        E2E_S3TABLESVOLUME_PREFIX,
        E2E_S3TABLESVOLUME2_PREFIX,
        &["AWS_ARN", "NAMESPACE"],
    );

    let test_suffix = test_suffix();
    let client = create_s3tables_client(E2E_S3TABLESVOLUME2_PREFIX).await?;
    let bucket_arn = s3_tables_volume("", E2E_S3TABLESVOLUME2_PREFIX)?.arn; // get bucket from arn

    // Ignore deletion status
    let _ = delete_s3tables_bucket_table(
        &client,
        bucket_arn.clone(),
        TEST_SCHEMA_NAME.to_string(),
        "table_partial_create".to_string(),
    )
    .await
    .context(TestAwsSdkSnafu);

    set_table_bucket_policy(
        &client,
        bucket_arn.clone(),
        S3TABLES_BUCKET_DENY_READ_WRITE_POLICY_DATA.replace("__BUCKET_ARN__", &bucket_arn),
    )
    .await
    .context(TestAwsSdkSnafu)?;

    // Currently embucket can only read database from s3tables volume when created before executor
    let exec = create_executor_with_early_volumes_creation(
        ObjectStoreType::File(test_suffix.clone(), env::temp_dir().join("store")),
        "memory_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_S3TABLESVOLUME2_PREFIX), // Note: prefix is different, it contains other bucket
            volume_type: TestVolumeType::S3Tables,
            volume: "volume_s3tables",
            database: "database_in_s3tables",
            schema: TEST_SCHEMA_NAME,
        }],
    )
    .await?;
    let exec = Arc::new(exec);

    // create tables & assign separate read, write policies
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE IF NOT EXISTS __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA IF NOT EXISTS __DATABASE__.__SCHEMA__",
            "CREATE TABLE IF NOT EXISTS __DATABASE__.__SCHEMA__.table_partial_create(amount number, name string, c5 VARCHAR)",
        ],
        executor: exec.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3Tables]).await?);

    // temp test
    // Here use freshly created sessions instead of precreated
    let session3 = "session3";
    exec.executor
        .create_session(session3)
        .await
        .expect("Failed to create session3");

    // Create new executor that fails as of partially created table
    let _ = create_executor_with_early_volumes_creation(
        ObjectStoreType::File(test_suffix, env::temp_dir().join("store")),
        "memory_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_S3TABLESVOLUME2_PREFIX), // Note: prefix is different, it contains other bucket
            volume_type: TestVolumeType::S3Tables,
            volume: "volume_s3tables",
            database: "database_in_s3tables",
            schema: TEST_SCHEMA_NAME,
        }],
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_two_executors_unrelated_inserts_ok() -> Result<(), Error> {
    eprintln!(
        "Test creates a table and then simultaneously runs insert and select queries in separate sessions. \
        Both requests should pass."
    );
    dotenv().ok();

    let test_suffix1 = test_suffix();
    let test_suffix2 = test_suffix();

    let file_exec1 = create_executor(
        ObjectStoreType::File(test_suffix1.clone(), env::temp_dir().join("store")),
        "#1",
    )
    .await?;
    let file_exec1 = Arc::new(file_exec1);

    let file_exec2 = create_executor(
        ObjectStoreType::File(test_suffix2.clone(), env::temp_dir().join("store")),
        "#2",
    )
    .await?;
    let file_exec2 = Arc::new(file_exec2);

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                INSERT_INTO_ALL_SNOWFLAKE_TYPES,
            ],
            executor: file_exec1,
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                CREATE_TABLE_WITH_ALL_SNOWFLAKE_TYPES,
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux');",
            ],
            executor: file_exec2,
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
    ])];

    assert!(
        exec_parallel_test_plan(
            test_plan,
            &[
                TestVolumeType::Memory,
                TestVolumeType::File,
                TestVolumeType::S3
            ]
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_s3volume_single_executor_two_sessions_one_session_inserts_other_selects()
-> Result<(), Error> {
    eprintln!(
        "This test runs two unrelated insert queries in separate Embucket executors. \
        Both should pass."
    );
    dotenv().ok();

    let test_suffix = test_suffix();

    let s3_exec = create_executor(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_env(MINIO_OBJECT_STORE_PREFIX)?,
        ),
        "s3_exec",
    )
    .await?;
    let s3_exec = Arc::new(s3_exec);

    let test_plan = vec![
        ParallelTest(vec![TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            ],
            executor: s3_exec.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        }]),
        ParallelTest(vec![
            TestQuery {
                sqls: vec![
                    "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux')",
                ],
                executor: s3_exec.clone(),
                session_id: TEST_SESSION_ID2,
                expected_res: true,
                err_callback: None,
            },
            TestQuery {
                sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
                executor: s3_exec,
                session_id: TEST_SESSION_ID2,
                expected_res: true,
                err_callback: None,
            },
        ]),
    ];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_bad_aws_creds_s3_volume_insert_should_fail()
-> Result<(), Error> {
    eprintln!(
        "Test creates a table and then corrupts the S3 volume credentials. \
        It verifies that insert operations fail with the corrupted credentials. \
        Note: The error output may not be clean due to issues with downcasting ObjectStore errors."
    );
    dotenv().ok();

    let executor = create_executor(
        ObjectStoreType::File(test_suffix(), env::temp_dir().join("store")),
        "file_exec",
    )
    .await?;
    let executor = Arc::new(executor);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds(None).await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
        err_callback: None,
    }])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_metastore_bad_aws_creds() -> Result<(), Error> {
    const E2E_BAD_METASTORE_AWS_CREDS_PREFIX: &str = "E2E_BAD_METASTORE_AWS_CREDS_";
    eprintln!("Test creates executor using bad credentials from Metastore. It fails.");
    dotenv().ok();

    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        E2E_BAD_METASTORE_AWS_CREDS_PREFIX,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{E2E_BAD_METASTORE_AWS_CREDS_PREFIX}AWS_ACCESS_KEY_ID"),
            "1",
        );
        std::env::set_var(
            format!("{E2E_BAD_METASTORE_AWS_CREDS_PREFIX}AWS_SECRET_ACCESS_KEY"),
            "1",
        );
    }

    let _ = create_executor(
        ObjectStoreType::S3(
            test_suffix(),
            S3ObjectStore::from_env(E2E_BAD_METASTORE_AWS_CREDS_PREFIX)?,
        ),
        "s3_exec",
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_metastore_readonly_aws_creds() -> Result<(), Error> {
    const E2E_METASTORE_READONLY_PREFIX: &str = "E2E_METASTORE_READONLY_";
    eprintln!("Test creates executor using readonly credentials from Metastore. It fails.");
    dotenv().ok();

    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        E2E_METASTORE_READONLY_PREFIX,
        &["AWS_BUCKET"],
    );

    /*
    At this moment Deny Policies are:
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:PutObjectAcl",
        "s3:AbortMultipartUpload"

    Allow Policies are:
        "s3:GetObject",
        "s3:ListBucket"
    */

    let executor = create_executor(
        ObjectStoreType::S3(
            test_suffix(),
            S3ObjectStore::from_env(E2E_METASTORE_READONLY_PREFIX)?,
        ),
        "s3_exec",
    )
    .await?;
    let executor = Arc::new(executor);

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            sqls: vec!["CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__"],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec!["SHOW DATABASES"],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec!["SELECT 1"],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
    ])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_pure_aws_s3_volume_insert_fail_select_ok()
-> Result<(), Error> {
    const E2E_READONLY_S3VOLUME_PREFIX: &str = "E2E_READONLY_S3VOLUME_";

    eprintln!(
        "Test uses s3 bucket with read only permisisons for s3 volumes. \
        select should pass, insert should fail."
    );
    dotenv().ok();

    copy_env_to_new_prefix(
        AWS_OBJECT_STORE_PREFIX,
        E2E_READONLY_S3VOLUME_PREFIX,
        &["AWS_BUCKET"],
    );

    let executor = create_executor_with_early_volumes_creation(
        // use static suffix to reuse the same metastore every time for this test
        ObjectStoreType::S3(
            "static".to_string(),
            S3ObjectStore::from_env(AWS_OBJECT_STORE_PREFIX)?,
        ),
        "s3_readonly_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_READONLY_S3VOLUME_PREFIX),
            volume_type: TestVolumeType::S3,
            volume: "volume_s3",
            database: "read_only_database_in_s3",
            schema: "public",
        }],
    )
    .await?;
    let executor = Arc::new(executor);

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            sqls: vec![
                //
                // uncomment this once if schema bucket deleted but need to recreate a table
                //
                // "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                // "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                // "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                // "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                //         (100, 'Alice', 'foo'),
                //         (200, 'Bob', 'bar'),
                //         (300, 'Charlie', 'baz'),
                //         (400, 'Diana', 'qux'),
                //         (500, 'Eve', 'quux')",
                "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
            ],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
            err_callback: None,
        },
        TestQuery {
            sqls: vec![
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                        (100, 'Alice', 'foo')",
            ],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: false,
            err_callback: None,
        },
    ])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_bad_aws_creds_s3_volume_not_permitted_select_returns_data_behaviour()
-> Result<(), Error> {
    eprintln!(
        "This test creates data on an S3 volume and runs a select query. \
        Then it corrupts the S3 volume credentials and verifies that subsequent select queries fail. \
        Note: Currently, the select query is not failing as expected."
    );
    dotenv().ok();

    let executor = create_executor(
        ObjectStoreType::File(test_suffix(), env::temp_dir().join("store")),
        "file_exec",
    )
    .await?;
    let executor = Arc::new(executor);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds(None).await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
        err_callback: None,
    }])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_bad_aws_creds_s3_volume_select_fail()
-> Result<(), Error> {
    eprintln!(
        "This test creates data on an S3 volume, then creates a new executor with injected credential errors. \
        It verifies that select operations fail with the corrupted credentials."
    );
    dotenv().ok();

    let executor = create_executor(
        ObjectStoreType::File(test_suffix(), env::temp_dir().join("store")),
        "#1",
    )
    .await?;
    let executor = Arc::new(executor);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
            "CREATE SCHEMA __DATABASE__.__SCHEMA__",
            "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
        err_callback: None,
    }])];
    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    // This executor uses correct credentials by default
    let executor = create_executor(
        ObjectStoreType::File(test_suffix(), env::temp_dir().join("store")),
        "#2",
    )
    .await?;

    let executor = Arc::new(executor);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds(None).await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
        executor,
        session_id: TEST_SESSION_ID1,
        expected_res: false,
        err_callback: None,
    }])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_all_stores_single_executor_two_sessions_different_tables_inserts_should_pass()
-> Result<(), Error> {
    eprintln!(
        "This test runs a single Embucket instance with file-based and S3-based volumes across two sessions, \
        writing to different tables in each session."
    );
    dotenv().ok();

    let test_suffix = test_suffix();

    let executors = vec![
        create_executor(
            ObjectStoreType::File(test_suffix.clone(), env::temp_dir().join("store")),
            "file_exec",
        )
        .await?,
        create_executor(ObjectStoreType::Memory(test_suffix.clone()), "memory_exec").await?,
        create_executor(
            ObjectStoreType::S3(
                test_suffix.clone(),
                S3ObjectStore::from_env(MINIO_OBJECT_STORE_PREFIX)?,
            ),
            "s3_exec",
        )
        .await?,
    ];

    for executor in executors {
        // test every executor sequentially but their sessions in parallel
        let executor = Arc::new(executor);

        let test_plan = vec![
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
                err_callback: None,
            }]),
            ParallelTest(vec![
                TestQuery {
                    sqls: vec![
                        INSERT_INTO_ALL_SNOWFLAKE_TYPES, // last query runs in non blocking mode
                    ],
                    executor: executor.clone(),
                    session_id: TEST_SESSION_ID1,
                    expected_res: true,
                    err_callback: None,
                },
                TestQuery {
                    sqls: vec![
                        // test if database and schema table created in other sessions can be resolved in this session
                        "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux');",
                    ],
                    executor,
                    session_id: TEST_SESSION_ID2, // reuse template for either two sessions or two executors
                    expected_res: true,
                    err_callback: None,
                },
            ]),
        ];

        assert!(
            exec_parallel_test_plan(
                test_plan,
                &[
                    TestVolumeType::S3,
                    TestVolumeType::File,
                    TestVolumeType::Memory
                ]
            )
            .await?
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_with_old_and_freshly_created_sessions_file_s3_volumes()
-> Result<(), Error> {
    eprintln!(
        "This test verifies object access across sessions with different lifecycles. \
        The first session creates objects, then tests access from both pre-existing and newly created sessions. \
        The test uses both file and S3-based volumes."
    );
    dotenv().ok();

    template_test_s3_store_single_executor_with_old_and_freshly_created_sessions(&[
        TestVolumeType::File,
        TestVolumeType::S3,
    ])
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_with_old_and_freshly_created_sessions_memory_volume()
-> Result<(), Error> {
    eprintln!(
        "This test verifies object access across sessions with different lifecycles. \
        The first session creates objects, then tests access from both pre-existing and newly created sessions. \
        The test uses an in-memory volume."
    );
    dotenv().ok();

    template_test_s3_store_single_executor_with_old_and_freshly_created_sessions(&[
        TestVolumeType::Memory,
    ])
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_same_file_object_store_two_executors_first_reads_second_writes_fails()
-> Result<(), Error> {
    eprintln!(
        "This test demonstrates that after creating a second executor, the first one fails on write operations."
    );
    dotenv().ok();

    let test_suffix = test_suffix();

    let object_store_file = ObjectStoreType::File(test_suffix, env::temp_dir().join("store"));

    let file_exec1 = create_executor(object_store_file.clone(), "#1").await?;
    let _ = create_executor(object_store_file, "#2").await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec!["CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__"],
        executor: Arc::new(file_exec1),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
        err_callback: None,
    }])];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);
    Ok(())
}

// Two embucket instances, both writers, one succeed with writing, other should fail
// Two embucket instances with shared s3 based configuration, second instance should read first instance writes
#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_same_file_object_store_two_executors_first_fenced_second_writes_ok()
-> Result<(), Error> {
    eprintln!(
        "This test creates data using one executor, then creates a second executor. \
        The second executor becomes the single writer, while the first executor can only read and \
        receives a 'Fenced' error on any write attempt."
    );
    dotenv().ok();

    template_test_two_executors_file_object_store_one_writer_fences_another(
        &[TestVolumeType::S3],
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_same_file_object_store_two_executors_first_fenced_second_after_delay_fails()
-> Result<(), Error> {
    eprintln!(
        "This test creates data using one executor, then creates a second executor. \
        The second executor becomes the single writer, while the first executor can only read and \
        receives a 'Fenced' error on any write attempt. \
        This test adds a delay after creating the second executor and before the first \
        executor starts any SQL operations."
    );
    dotenv().ok();

    template_test_two_executors_file_object_store_one_writer_fences_another(
        &[TestVolumeType::S3],
        Some(Duration::from_secs(11)),
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_create_volume_with_non_existing_bucket() -> Result<(), Error> {
    const E2E_S3VOLUME_NON_EXISTING_BUCKET_PREFIX: &str = "E2E_S3VOLUME_NON_EXISTING_BUCKET_";

    eprintln!("Create s3 volume with non existing bucket");
    dotenv().ok();

    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        E2E_S3VOLUME_NON_EXISTING_BUCKET_PREFIX,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{E2E_S3VOLUME_NON_EXISTING_BUCKET_PREFIX}AWS_BUCKET"),
            "non_existing_bucket",
        );
    }

    let test_suffix = test_suffix();

    let s3_exec = create_executor_with_early_volumes_creation(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_env(MINIO_OBJECT_STORE_PREFIX)?,
        ),
        "s3_exec",
        vec![VolumeConfig {
            prefix: Some(E2E_S3VOLUME_NON_EXISTING_BUCKET_PREFIX),
            volume_type: TestVolumeType::S3,
            volume: "s3_volume_with_existing_bucket",
            database: "db",
            schema: "schema",
        }],
    )
    .await?;
    let s3_exec = Arc::new(s3_exec);

    let test_plan = vec![
        ParallelTest(vec![TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                "SHOW DATABASES",
                "SHOW SCHEMAS",
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
            ],
            executor: s3_exec.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: false,
            err_callback: None,
        }]),
        ParallelTest(vec![TestQuery {
            sqls: vec![
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                (100, 'Alice', 'foo'),
                (200, 'Bob', 'bar'),
                (300, 'Charlie', 'baz'),
                (400, 'Diana', 'qux'),
                (500, 'Eve', 'quux')",
            ],
            executor: s3_exec.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: false,
            err_callback: None,
        }]),
    ];

    assert!(exec_parallel_test_plan(test_plan, &[TestVolumeType::S3]).await?);

    Ok(())
}

// TODO: Consider what to do with such test
// we can't verify error type here is objectstore error or not, as of SlteDBError turned private
#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_create_executor_fails()
-> Result<(), Error> {
    let some_id = 1;
    let port = 9995;
    let bytes_limit = 1;
    let expected_res = false;

    eprintln!(
        "This spawns a test server with id={some_id} and emulates running queries in environment with unstable network. \
        It injects communication failure limit={bytes_limit} for executor writing/reading s3 on http://localhost:{port} \
        and makes ExecutionService creation fail as of ObjectStore error."
    );
    dotenv().ok();

    // prepare envs for object store
    let minio_object_store_toxic_prefix = format!("MINIO_OBJECT_STORE_TOXIC{some_id}_");
    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        &minio_object_store_toxic_prefix,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{minio_object_store_toxic_prefix}AWS_ENDPOINT"),
            format!("http://localhost:{port}"),
        );
    }

    let (minio_proxy_name, toxic_minio_proxy_payload) = toxiproxy_name_and_payload(some_id, port);
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let create_proxy_res = create_toxiproxy(&toxic_minio_proxy_payload).await?;
    eprintln!("create_proxy_res: {create_proxy_res:?}");
    let create_toxic_res = create_toxic_conn_limit(&minio_proxy_name, bytes_limit).await?;
    eprintln!("create_toxic_res: {create_toxic_res:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // inject error on any interaction with object store - creating executor will fail
    let res = template_s3_connections_test(
        minio_object_store_toxic_prefix, // metastore conn is poisoned
        E2E_S3VOLUME_PREFIX.to_string(), // s3 volume conn is not poisoned
        expected_res,
        expected_res,
        None,
    )
    .await;

    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors

    assert!(res.is_err());
    if let Err(e) = &res {
        // Since slatedb v0.8 SlateDbError is private, objectstore error can't be downcasted anymore.
        // error happended in creating ExecutionService is internal, so do not check error type itself
        // Error::TestSlatedb {
        //     source: slatedb::error::SlateDBError::ObjectStoreError(_object_store),
        panic!("Expected other error, Actual error: {e}");
    }

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_write_to_metastore_create_table_fails()
-> Result<(), Error> {
    struct ErrCallback;
    impl TestQueryCallback for ErrCallback {
        fn err_callback(&self, err: &crate::Error) {
            let snowflake_err = err.to_snowflake_error();
            assert_eq!(snowflake_err.error_code(), ErrorCode::ObjectStore);
            assert!(
                snowflake_err
                    .to_string()
                    .starts_with("Iceberg Metastore Db Object store: Generic S3 error:")
            );
        }
    }

    let some_id = 2;
    let port = 9996;
    // this is fragile method, but following is reasnable too:
    // 1. when it triggers limit on adding query to history - error is only will send to logs and won't affect result
    // 2. when we create huge table which SQL already bigger than connection limit set with toxic
    // But in overal conection won't break, for unknown reason. Though sometimes it does.
    let bytes_limit = 10_000;
    let expected_res = false;

    eprintln!(
        "This spawns a test server with id={some_id} and emulates running queries in environment with unstable network. \
        It injects communication failure limit={bytes_limit} for executor writing/reading s3 on http://localhost:{port} \
        remote metadata read/write should fail as of ObjectStore error."
    );
    dotenv().ok();

    // prepare envs for object store
    let minio_object_store_toxic_prefix = format!("MINIO_OBJECT_STORE_TOXIC{some_id}_");
    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        &minio_object_store_toxic_prefix,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{minio_object_store_toxic_prefix}AWS_ENDPOINT"),
            format!("http://localhost:{port}"),
        );
    }

    let (minio_proxy_name, toxic_minio_proxy_payload) = toxiproxy_name_and_payload(some_id, port);
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let create_proxy_res = create_toxiproxy(&toxic_minio_proxy_payload).await?;
    eprintln!("create_proxy_res: {create_proxy_res:?}");
    let create_toxic_res = create_toxic_conn_limit(&minio_proxy_name, bytes_limit).await?;
    eprintln!("create_toxic_res: {create_toxic_res:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // inject error when creating schema - as it is relatively small data, it should overpass creating executor
    // but fail in creating schema
    let res = template_s3_connections_test(
        minio_object_store_toxic_prefix, // metastore conn is poisoned
        E2E_S3VOLUME_PREFIX.to_string(), // s3 volume conn is not poisoned
        true,
        expected_res,
        Some(Box::new(ErrCallback)),
    )
    .await;

    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors

    assert!(res?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_s3_volume_write_create_table_fails()
-> Result<(), Error> {
    const E2E_S3VOLUME_TOXIC_PREFIX: &str = "E2E_S3VOLUME_TOXIC_";

    struct ErrCallback;
    impl TestQueryCallback for ErrCallback {
        fn err_callback(&self, err: &crate::Error) {
            let snowflake_err = err.to_snowflake_error();
            assert_eq!(snowflake_err.error_code(), ErrorCode::ObjectStore);
            assert!(
                snowflake_err
                    .to_string()
                    .starts_with("Iceberg Metastore Object store: Generic S3 error:")
            );
        }
    }

    let some_id = 3;
    let port = 9997;
    let bytes_limit = 100;
    let create_executor_res = true;
    let sql_res = false;

    eprintln!(
        "This spawns a test server with id={some_id} and emulates running queries in environment with unstable network. \
        It injects communication failure limit={bytes_limit} for executor writing/reading s3 on http://localhost:{port} \
        s3 volume read/write should fail as of ObjectStore error."
    );
    dotenv().ok();

    // prepare envs for volumes
    copy_env_to_new_prefix(E2E_S3VOLUME_PREFIX, E2E_S3VOLUME_TOXIC_PREFIX, &[]);
    unsafe {
        std::env::set_var(
            format!("{E2E_S3VOLUME_TOXIC_PREFIX}AWS_ENDPOINT"),
            format!("http://localhost:{port}"),
        );
    }

    let (minio_proxy_name, toxic_minio_proxy_payload) = toxiproxy_name_and_payload(some_id, port);
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let create_proxy_res = create_toxiproxy(&toxic_minio_proxy_payload).await?;
    eprintln!("create_proxy_res: {create_proxy_res:?}");
    let create_toxic_res = create_toxic_conn_limit(&minio_proxy_name, bytes_limit).await?;
    eprintln!("create_toxic_res: {create_toxic_res:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // inject error on data insert into s3 volume
    let res = template_s3_connections_test(
        MINIO_OBJECT_STORE_PREFIX.to_string(), // metastore conn is not poisoned
        E2E_S3VOLUME_TOXIC_PREFIX.to_string(), // s3 volume conn is poisoned
        create_executor_res,
        sql_res,
        Some(Box::new(ErrCallback)),
    )
    .await;

    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors

    assert!(res?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_write_to_metastore_create_database_fails()
-> Result<(), Error> {
    struct ErrCallback;
    impl TestQueryCallback for ErrCallback {
        fn err_callback(&self, err: &crate::Error) {
            let snowflake_err = err.to_snowflake_error();
            assert_eq!(snowflake_err.error_code(), ErrorCode::ObjectStore);
            assert!(
                snowflake_err
                    .to_string()
                    .starts_with("Catalog Metastore Db Object store: Generic S3 error:")
            );
        }
    }

    let some_id = 4;
    let port = 9998;
    // this is fragile method, but following is reasnable too:
    // 1. when it triggers limit on adding query to history - error is only will send to logs and won't affect result
    // 2. when we create huge table which SQL already bigger than connection limit set with toxic
    // But in overal conection won't break, for unknown reason. Though sometimes it does.
    let bytes_limit = 10_000;
    let expected_res = false;

    eprintln!(
        "This spawns a test server with id={some_id} and emulates running queries in environment with unstable network. \
        It injects communication failure limit={bytes_limit} for executor writing/reading s3 on http://localhost:{port} \
        remote metadata read/write should fail as of ObjectStore error."
    );
    dotenv().ok();

    // prepare envs for object store
    let minio_object_store_toxic_prefix = format!("MINIO_OBJECT_STORE_TOXIC{some_id}_");
    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        &minio_object_store_toxic_prefix,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{minio_object_store_toxic_prefix}AWS_ENDPOINT"),
            format!("http://localhost:{port}"),
        );
    }

    let (minio_proxy_name, toxic_minio_proxy_payload) = toxiproxy_name_and_payload(some_id, port);
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let create_proxy_res = create_toxiproxy(&toxic_minio_proxy_payload).await?;
    eprintln!("create_proxy_res: {create_proxy_res:?}");
    let create_toxic_res = create_toxic_conn_limit(&minio_proxy_name, bytes_limit).await?;
    eprintln!("create_toxic_res: {create_toxic_res:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // inject error when creating schema - as it is relatively small data, it should overpass creating executor
    // but fail in creating schema
    let res = template_s3_connections_test2(
        minio_object_store_toxic_prefix, // metastore conn is poisoned
        E2E_S3VOLUME_PREFIX.to_string(), // s3 volume conn is not poisoned
        true,
        expected_res,
        Some(Box::new(ErrCallback)),
    )
    .await;

    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors

    assert!(res?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_write_to_metastore_create_schema_fails()
-> Result<(), Error> {
    struct ErrCallback;
    impl TestQueryCallback for ErrCallback {
        fn err_callback(&self, err: &crate::Error) {
            let snowflake_err = err.to_snowflake_error();
            assert_eq!(snowflake_err.error_code(), ErrorCode::ObjectStore);
            assert!(
                snowflake_err
                    .to_string()
                    .starts_with("Iceberg Metastore Db Object store: Generic S3 error:")
            );
        }
    }

    let some_id = 5;
    let port = 9999;
    // this is fragile method, but following is reasnable too:
    // 1. when it triggers limit on adding query to history - error is only will send to logs and won't affect result
    // 2. when we create huge table which SQL already bigger than connection limit set with toxic
    // But in overal conection won't break, for unknown reason. Though sometimes it does.
    let bytes_limit = 10_000;
    let expected_res = false;

    eprintln!(
        "This spawns a test server with id={some_id} and emulates running queries in environment with unstable network. \
        It injects communication failure limit={bytes_limit} for executor writing/reading s3 on http://localhost:{port} \
        remote metadata read/write should fail as of ObjectStore error."
    );
    dotenv().ok();

    // prepare envs for object store
    let minio_object_store_toxic_prefix = format!("MINIO_OBJECT_STORE_TOXIC{some_id}_");
    copy_env_to_new_prefix(
        MINIO_OBJECT_STORE_PREFIX,
        &minio_object_store_toxic_prefix,
        &[],
    );
    unsafe {
        std::env::set_var(
            format!("{minio_object_store_toxic_prefix}AWS_ENDPOINT"),
            format!("http://localhost:{port}"),
        );
    }

    let (minio_proxy_name, toxic_minio_proxy_payload) = toxiproxy_name_and_payload(some_id, port);
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let create_proxy_res = create_toxiproxy(&toxic_minio_proxy_payload).await?;
    eprintln!("create_proxy_res: {create_proxy_res:?}");
    let create_toxic_res = create_toxic_conn_limit(&minio_proxy_name, bytes_limit).await?;
    eprintln!("create_toxic_res: {create_toxic_res:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // inject error when creating schema - as it is relatively small data, it should overpass creating executor
    // but fail in creating schema
    let res = template_s3_connections_test3(
        minio_object_store_toxic_prefix, // metastore conn is poisoned
        E2E_S3VOLUME_PREFIX.to_string(), // s3 volume conn is not poisoned
        true,
        expected_res,
        Some(Box::new(ErrCallback)),
    )
    .await;

    let _ = delete_toxic_conn_limit(&minio_proxy_name).await; // ignore deletion errors
    let _ = delete_toxiproxy(&minio_proxy_name).await; // ignore deletion errors

    assert!(res?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_s3_store_single_executor_s3_connection_issues_false_positive_check()
-> Result<(), Error> {
    eprintln!(
        "This spawns a test server that runs without introducing connection issues, \
        if it fails that means all the related tests must be broken too."
    );
    dotenv().ok();

    template_s3_connections_test(
        MINIO_OBJECT_STORE_PREFIX.to_string(), // metastore conn is not poisoned
        E2E_S3VOLUME_PREFIX.to_string(),       // s3 volume conn is not poisoned
        true,
        true,
        None,
    )
    .await?;
    Ok(())
}

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
