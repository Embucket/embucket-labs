#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
use crate::service::ExecutionService;
use crate::tests::e2e_common::{
    Error,
    ObjectStoreType,
    ParallelTest,
    S3ObjectStore,
    TEST_SESSION_ID1,
    TEST_SESSION_ID2,
    TEST_VOLUME_FILE,
    TEST_VOLUME_MEMORY,
    TEST_VOLUME_S3,
    // TEST_VOLUME_S3TABLES
    TestQuery,
    create_executor,
    exec_parallel_test_plan,
    test_suffix,
};
use dotenv::dotenv;
use std::env;
use std::sync::Arc;
use std::time::Duration;

async fn template_test_two_executors_file_object_store_one_writer_fences_another(
    volumes: &[(&str, &str)],
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
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec()).await?);

    // create 2nd executor on the same object store
    let file_exec2 = create_executor(object_store_file, "#2").await?;
    let file_exec2 = Arc::new(file_exec2);

    // write data using 2nd executor
    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: file_exec2.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec()).await?);

    // give delay for sync job to run
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await; // Ensure the executor is created after the previous delay
    }

    let test_plan = vec![ParallelTest(vec![TestQuery {
        // After being fenced:
        sqls: vec![
            // first executor still successfully reads data
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: file_exec1.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: true,
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec()).await?);

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            // After being fenced:
            sqls: vec![
                // first executor fails to write
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                (100, 'Alice', 'foo')",
            ],
            executor: file_exec1.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: false,
        },
        TestQuery {
            sqls: vec![
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
            ],
            executor: file_exec2,
            session_id: TEST_SESSION_ID1,
            expected_res: true,
        },
    ])];

    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec()).await?);

    let test_plan = vec![ParallelTest(vec![TestQuery {
        // After being fenced:
        sqls: vec![
            // first executor still successfully reads data
            "SELECT * FROM __DATABASE__.__SCHEMA__.hello",
        ],
        executor: file_exec1,
        session_id: TEST_SESSION_ID1,
        expected_res: true,
    }])];
    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec()).await?);

    Ok(())
}

async fn template_test_s3_store_single_executor_with_old_and_freshly_created_sessions(
    volumes: &[(&str, &str)],
) -> Result<(), Error> {
    let executor = create_executor(
        ObjectStoreType::S3(test_suffix(), S3ObjectStore::from_env()),
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
    }])];
    assert!(exec_parallel_test_plan(prerequisite_test, volumes.to_vec(),).await?);

    // Here use freshly created sessions instead of precreated
    let newly_created_session = "newly_created_session";
    executor
        .executor
        .create_session(newly_created_session.to_string())
        .await
        .expect("Failed to create newly_created_session");

    let test_plan = vec![ParallelTest(vec![
        TestQuery {
            sqls: vec![INSERT_INTO_ALL_SNOWFLAKE_TYPES],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
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
        },
    ])];

    assert!(exec_parallel_test_plan(test_plan, volumes.to_vec(),).await?);
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
        },
    ])];

    assert!(
        exec_parallel_test_plan(
            test_plan,
            vec![
                TEST_VOLUME_MEMORY,
                TEST_VOLUME_FILE,
                TEST_VOLUME_S3, /*TEST_VOLUME_S3TABLES*/
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
        ObjectStoreType::S3(test_suffix.clone(), S3ObjectStore::from_env()),
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
            },
            TestQuery {
                sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
                executor: s3_exec,
                session_id: TEST_SESSION_ID2,
                expected_res: true,
            },
        ]),
    ];

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);
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
    }])];
    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds().await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec![
            "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES
                    (100, 'Alice', 'foo')",
        ],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
    }])];

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_bad_aws_creds_s3_volume_select_should_fail()
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
    }])];
    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds().await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
        executor: executor.clone(),
        session_id: TEST_SESSION_ID1,
        expected_res: false,
    }])];

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

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
    }])];
    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

    // This executor uses correct credentials by default
    let executor = create_executor(
        ObjectStoreType::File(test_suffix(), env::temp_dir().join("store")),
        "#2",
    )
    .await?;

    let executor = Arc::new(executor);

    // corrupt s3 volume
    executor.create_s3_volume_with_bad_creds().await?;

    let test_plan = vec![ParallelTest(vec![TestQuery {
        sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
        executor,
        session_id: TEST_SESSION_ID1,
        expected_res: false,
    }])];

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);

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
            ObjectStoreType::S3(test_suffix.clone(), S3ObjectStore::from_env()),
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
            }]),
            ParallelTest(vec![
                TestQuery {
                    sqls: vec![
                        INSERT_INTO_ALL_SNOWFLAKE_TYPES, // last query runs in non blocking mode
                    ],
                    executor: executor.clone(),
                    session_id: TEST_SESSION_ID1,
                    expected_res: true,
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
                },
            ]),
        ];

        assert!(
            exec_parallel_test_plan(
                test_plan,
                vec![TEST_VOLUME_S3, TEST_VOLUME_FILE, TEST_VOLUME_MEMORY,]
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
        TEST_VOLUME_FILE,
        TEST_VOLUME_S3,
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
        TEST_VOLUME_MEMORY,
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
    }])];

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);
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
        &[TEST_VOLUME_S3],
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_same_file_object_store_two_executors_first_fenced_second_fails_if_delayed_is_this_needed()
-> Result<(), Error> {
    eprintln!(
        "This test creates data using one executor, then creates a second executor. \
        The second executor becomes the single writer, while the first executor can only read and \
        receives a 'Fenced' error on any write attempt. \
        This test includes an additional delay after creating the second executor and before the first executor starts any SQL operations."
    );
    dotenv().ok();

    template_test_two_executors_file_object_store_one_writer_fences_another(
        &[TEST_VOLUME_S3],
        Some(Duration::from_secs(11)),
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
