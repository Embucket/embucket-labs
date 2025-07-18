#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
use crate::tests::e2e_common::{
    Error, ExecutorWithObjectStore, ObjectStoreType, ParallelTest, S3ObjectStore, TEST_SESSION_ID1,
    TEST_SESSION_ID2, TEST_VOLUME_FILE, TEST_VOLUME_MEMORY, TEST_VOLUME_S3, TestQuery,
    create_executor, exec_parallel_test_plan, test_suffix,
};
use dotenv::dotenv;
use std::env;
use std::sync::Arc;

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

pub fn template_two_executors_both_write_different_tables(
    executor1: Arc<ExecutorWithObjectStore>,
    executor2: Arc<ExecutorWithObjectStore>,
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
            executor: executor1.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
        }]),
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
                session_id: TEST_SESSION_ID2, // reuse template for either two sessions or two executors
                expected_res: true,
            },
        ]),
    ]
}

pub fn template_single_executor_two_sessions_one_session_inserts_other_selects(
    executor: Arc<ExecutorWithObjectStore>,
) -> Vec<ParallelTest> {
    // Running single Embucket (all volumes), two sessions, writes to different tables
    vec![
        ParallelTest(vec![TestQuery {
            sqls: vec![
                "CREATE DATABASE __DATABASE__ EXTERNAL_VOLUME = __VOLUME__",
                "CREATE SCHEMA __DATABASE__.__SCHEMA__",
                "CREATE TABLE __DATABASE__.__SCHEMA__.hello(amount number, name string, c5 VARCHAR)",
                "INSERT INTO __DATABASE__.__SCHEMA__.hello (amount, name, c5) VALUES 
                        (100, 'Alice', 'foo'),
                        (200, 'Bob', 'bar'),
                        (300, 'Charlie', 'baz'),
                        (400, 'Diana', 'qux'),
                        (500, 'Eve', 'quux')",
            ],
            executor: executor.clone(),
            session_id: TEST_SESSION_ID1,
            expected_res: true,
        }]),
        ParallelTest(vec![TestQuery {
            sqls: vec!["SELECT * FROM __DATABASE__.__SCHEMA__.hello"],
            executor,
            session_id: TEST_SESSION_ID2,
            expected_res: true,
        }]),
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
async fn test_e2e_file_store_two_executors_unrelated_inserts() -> Result<(), Error> {
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
async fn test_e2e_s3_store_s3volume_single_executor_two_sessions_one_session_inserts_other_selects()
-> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    let s3_exec = create_executor(
        ObjectStoreType::S3(
            test_suffix.clone(),
            S3ObjectStore::from_prefixed_env("E2E_STORE"),
        ),
        &test_suffix,
    )
    .await?;

    let test_plan =
        template_single_executor_two_sessions_one_session_inserts_other_selects(Arc::new(s3_exec));

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_file_store_single_executor_two_sessions_different_tables_inserts()
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
    let file_exec = Arc::new(file_exec);

    // let test_plan =
    //    template_single_executor_two_sessions_different_tables_inserts(Arc::new(file_exec));
    let test_plan =
        template_two_executors_both_write_different_tables(file_exec.clone(), file_exec);

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
async fn test_e2e_memory_store_single_executor_two_sessions_different_tables_inserts()
-> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    // let storages = vec![
    //     ObjectStoreType::Memory,
    //     ObjectStoreType::File(test_suffix.clone()),
    //     ObjectStoreType::S3(S3ObjectStore::from_prefixed_env("E2E_STORE")),
    // ];

    let memory_exec = create_executor(ObjectStoreType::Memory, &test_suffix).await?;
    let memory_exec = Arc::new(memory_exec);

    // let test_plan =
    //    template_single_executor_two_sessions_different_tables_inserts(Arc::new(memory_exec));
    let test_plan =
        template_two_executors_both_write_different_tables(memory_exec.clone(), memory_exec);

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
async fn test_e2e_s3_store_single_executor_two_sessions_different_tables_inserts()
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
    let s3_exec = Arc::new(s3_exec);

    // let test_plan =
    //    template_single_executor_two_sessions_different_tables_inserts(Arc::new(s3_exec));
    let test_plan = template_two_executors_both_write_different_tables(s3_exec.clone(), s3_exec);

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
// async fn test_e2e_same_file_object_store_two_executors_create_table() -> Result<(), Error> {
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
// async fn test_e2e_same_file_object_store_two_executors_fail() -> Result<(), Error> {
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

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_e2e_same_file_object_store_two_executors_inserts() -> Result<(), Error> {
    dotenv().ok();

    let test_suffix = test_suffix();

    let object_store_file =
        ObjectStoreType::File(test_suffix.clone(), env::temp_dir().join("store"));

    let file_exec1 = create_executor(object_store_file.clone(), &test_suffix).await?;
    let file_exec2 = create_executor(object_store_file, &test_suffix).await?;

    let test_plan = template_two_executors_both_write_different_tables(
        Arc::new(file_exec1),
        Arc::new(file_exec2),
    );

    assert!(exec_parallel_test_plan(test_plan, vec![TEST_VOLUME_S3]).await?);
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
