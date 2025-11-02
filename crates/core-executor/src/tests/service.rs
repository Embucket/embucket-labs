use crate::Error;
use crate::models::{QueryContext, QueryResult};
use crate::running_queries::RunningQueryId;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use core_history::QueryStatus;
use core_history::SlateDBHistoryStore;
use core_history::entities::worksheet::Worksheet;
use core_history::{GetQueriesParams, HistoryStore};
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::models::table::TableIdent as MetastoreTableIdent;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume,
};
use datafusion::{arrow::csv::reader::Format, assert_batches_eq};
use futures::future::join_all;
use std::sync::Arc;

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_execute_always_returns_schema() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc =
        CoreExecutionService::new(metastore, history_store, Arc::new(Config::default()))
            .await
            .expect("Failed to create execution service");

    execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    let columns = execution_svc
        .query(
            "test_session_id",
            "SELECT 1 AS a, 2.0 AS b, '3' AS c WHERE False",
            QueryContext::default(),
        )
        .await
        .expect("Failed to execute query")
        .column_info();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].r#type, "fixed");
    assert_eq!(columns[1].r#type, "fixed");
    assert_eq!(columns[2].r#type, "text");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_service_upload_file() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let volume = metastore
        .create_volume(MetastoreVolume::new("test_volume".to_string(), core_metastore::VolumeType::Memory))
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(MetastoreDatabase::new("embucket".to_string(), volume.id))
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let file_name = "test.csv";
    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };

    // Create CSV data in memory
    let csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300";
    let data = csv_content.as_bytes().to_vec();

    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .expect("Failed to create execution service");

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id)
        .await
        .expect("Failed to create session");

    let csv_format = Format::default().with_header(true);
    let rows_loaded = execution_svc
        .upload_data_to_table(
            session_id,
            &table_ident,
            data.clone().into(),
            file_name,
            csv_format.clone(),
        )
        .await
        .expect("Failed to upload file");
    assert_eq!(rows_loaded, 3);

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let QueryResult { records, .. } = execution_svc
        .query(session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &records
    );

    let rows_loaded = execution_svc
        .upload_data_to_table(session_id, &table_ident, data.into(), file_name, csv_format)
        .await
        .expect("Failed to upload file");
    assert_eq!(rows_loaded, 3);

    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("SELECT * FROM {}", table_ident.table);
    let QueryResult { records, .. } = execution_svc
        .query(session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | name  | value |",
            "+----+-------+-------+",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "| 1  | test1 | 100   |",
            "| 2  | test2 | 200   |",
            "| 3  | test3 | 300   |",
            "+----+-------+-------+",
        ],
        &records
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_service_create_table_file_volume() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);

    // Create a temporary directory for the file volume
    let temp_dir = std::env::temp_dir().join("test_file_volume");
    let _ = std::fs::create_dir_all(&temp_dir);
    let temp_path = temp_dir.to_str().expect("Failed to convert path to string");
    let volume = metastore
        .create_volume(
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::File(core_metastore::FileVolume {
                    path: temp_path.to_string(),
                }),
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume_id: volume.id,
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .expect("Failed to create execution service");

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id)
        .await
        .expect("Failed to create session");

    let create_table_sql = format!(
        "CREATE TABLE {table_ident} (id INT, name STRING, value FLOAT) as VALUES (1, 'test1', 100.0), (2, 'test2', 200.0), (3, 'test3', 300.0)"
    );
    let QueryResult { records, .. } = execution_svc
        .query(session_id, &create_table_sql, QueryContext::default())
        .await
        .expect("Failed to create table");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &records
    );

    let insert_sql = format!(
        "INSERT INTO {table_ident} (id, name, value) VALUES (4, 'test4', 400.0), (5, 'test5', 500.0)"
    );
    let QueryResult { records, .. } = execution_svc
        .query(session_id, &insert_sql, QueryContext::default())
        .await
        .expect("Failed to insert data");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &records
    );
}

#[tokio::test]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn test_query_recording() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let volume = metastore
        .create_volume(
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");

    let database_name = "embucket".to_string();

    let database = metastore
        .create_database(
            MetastoreDatabase::new(database_name.clone(), volume.id),
        )
        .await
        .expect("Failed to create database");

    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .expect("Failed to create execution service");

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id)
        .await
        .expect("Failed to create session");

    let schema_name = "public".to_string();

    let context = QueryContext::new(Some(database_name.clone()), Some(schema_name.clone()), None);

    //Good query
    execution_svc
        .query(
            session_id,
            format!(
                "CREATE SCHEMA {}.{}",
                database_name.clone(),
                schema_name.clone()
            )
            .as_str(),
            context.clone(),
        )
        .await
        .expect("Failed to add schema");

    assert_eq!(
        1,
        history_store
            .get_queries(GetQueriesParams::default())
            .await
            .expect("Failed to get queries")
            .len()
    );

    //Failing query
    execution_svc
        .query(
            session_id,
            format!(
                "CREATE SCHEMA {}.{}",
                database_name.clone(),
                schema_name.clone()
            )
            .as_str(),
            context.clone(),
        )
        .await
        .expect_err("Failed to not add schema");

    assert_eq!(
        2,
        history_store
            .get_queries(GetQueriesParams::default())
            .await
            .expect("Failed to get queries")
            .len()
    );

    let table_name = "test1".to_string();

    //Create table queries
    execution_svc
        .query(
            session_id,
            format!(
                "create TABLE {}.{}.{}
        external_volume = ''
	    catalog = ''
	    base_location = ''
        (
	    APP_ID TEXT,
	    PLATFORM TEXT,
	    EVENT TEXT,
        TXN_ID NUMBER(38,0),
        EVENT_TIME TEXT
	    );",
                database_name.clone(),
                schema_name.clone(),
                table_name.clone()
            )
            .as_str(),
            context.clone(),
        )
        .await
        .expect("Failed to create table");

    assert_eq!(
        3,
        history_store
            .get_queries(GetQueriesParams::default())
            .await
            .expect("Failed to get queries")
            .len()
    );

    //Insert into query
    execution_svc
        .query(
            session_id,
            format!(
                "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT, TXN_ID, EVENT_TIME)
        VALUES ('12345', 'iOS', 'login', '123456', '2021-01-01T00:00:00'),
               ('67890', 'Android', 'purchase', '456789', '2021-01-01T00:02:00')",
                database_name.clone(),
                schema_name.clone(),
                table_name.clone()
            )
            .as_str(),
            context.clone(),
        )
        .await
        .expect("Failed to insert into");

    assert_eq!(
        4,
        history_store
            .get_queries(GetQueriesParams::default())
            .await
            .expect("Failed to get queries")
            .len()
    );

    //With worksheet
    let worksheet = history_store
        .add_worksheet(Worksheet::new("Testing1".to_string(), String::new()))
        .await
        .expect("Failed to add worksheet");

    assert_eq!(
        0,
        history_store
            .get_queries(GetQueriesParams::default().with_worksheet_id(worksheet.clone().id))
            .await
            .expect("Failed to get queries")
            .len()
    );

    execution_svc
        .query(
            session_id,
            format!(
                "INSERT INTO {}.{}.{} (APP_ID, PLATFORM, EVENT, TXN_ID, EVENT_TIME)
        VALUES ('1234', 'iOS', 'login', '123456', '2021-01-01T00:00:00'),
               ('6789', 'Android', 'purchase', '456789', '2021-01-01T00:02:00')",
                database_name.clone(),
                schema_name.clone(),
                table_name.clone()
            )
            .as_str(),
            QueryContext::new(
                Some(database_name.clone()),
                Some(schema_name.clone()),
                Some(worksheet.clone().id),
            ),
        )
        .await
        .expect("Failed to insert into");

    assert_eq!(
        1,
        history_store
            .get_queries(GetQueriesParams::default().with_worksheet_id(worksheet.clone().id))
            .await
            .expect("Failed to get queries")
            .len()
    );
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_max_concurrency_level() {
    use tokio::sync::Barrier;

    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = Arc::new(
        CoreExecutionService::new(
            metastore.clone(),
            history_store.clone(),
            Arc::new(Config::default().with_max_concurrency_level(2)),
        )
        .await
        .expect("Failed to create execution service"),
    );

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    let barrier = Arc::new(Barrier::new(3)); // wait for 3 threads: 2 queries + main thread

    // Reserve 2 permitted slots for the queries
    for _ in 0..2 {
        let svc = execution_svc.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let _ = svc
                .submit_query(
                    "test_session_id",
                    "SELECT sleep(2)",
                    QueryContext::default(),
                )
                .await;
            barrier.wait().await;
        });
        // add delay as miliseconds granularity used for query_id is not enough
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let res = execution_svc
        .query(
            "test_session_id",
            "SELECT sleep(3)",
            QueryContext::default(),
        )
        .await;
    assert!(
        res.is_err(),
        "Expected concurrency limit error but got {res:?}"
    );

    // Pass the barrier to allow the first two queries to finish
    barrier.wait().await;
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_max_concurrency_level2() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = Arc::new(
        CoreExecutionService::new(
            metastore.clone(),
            history_store.clone(),
            Arc::new(Config::default().with_max_concurrency_level(2)),
        )
        .await
        .expect("Failed to create execution service"),
    );

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    for _ in 0..2 {
        let _ = execution_svc
            .submit_query(
                "test_session_id",
                "SELECT sleep(2)",
                QueryContext::default(),
            )
            .await;
        // add delay as miliseconds granularity used for query_id is not enough
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let res = execution_svc
        .query("test_session_id", "SELECT 1", QueryContext::default())
        .await;
    assert!(
        res.is_err(),
        "Expected concurrency limit error but got {res:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used)]
#[allow(clippy::items_after_statements)]
async fn test_parallel_run() {
    const MAX_CONCURRENCY_LEVEL: usize = 10;
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = Arc::new(
        CoreExecutionService::new(
            metastore.clone(),
            history_store.clone(),
            Arc::new(Config::default().with_max_concurrency_level(MAX_CONCURRENCY_LEVEL)),
        )
        .await
        .expect("Failed to create execution service"),
    );

    let _ = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    async fn exec_query(
        execution_svc: Arc<dyn ExecutionService>,
        sql: &str,
    ) -> crate::Result<QueryResult> {
        execution_svc
            .query("test_session_id", sql, QueryContext::default())
            .await
    }

    let mut futures = Vec::new();
    for _ in 0..MAX_CONCURRENCY_LEVEL {
        let future = tokio::task::spawn(exec_query(execution_svc.clone(), "SELECT 1"));
        futures.push(future);
    }

    let results = join_all(futures)
        .await
        .into_iter()
        .map(|r| r.expect("Task panicked"))
        // .map(|_| Ok::<String, Error>(String::from("OK")))
        .collect::<Vec<_>>();
    let fails_count = results.iter().filter(|r| r.is_err()).count();
    eprintln!("queries results: {results:?}");
    assert_eq!(0, fails_count);
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_query_timeout() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = Arc::new(
        CoreExecutionService::new(
            metastore.clone(),
            history_store.clone(),
            Arc::new(Config::default().with_query_timeout(1)),
        )
        .await
        .expect("Failed to create execution service"),
    );

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    let res = execution_svc
        .query(
            "test_session_id",
            "SELECT sleep(3)",
            QueryContext::default(),
        )
        .await;
    assert!(
        res.is_err(),
        "Expected query execution exceeded timeout error but got {res:?}"
    );
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_submitted_query_timeout() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore,
        history_store.clone(),
        Arc::new(Config::default().with_query_timeout(1)),
    )
    .await
    .expect("Failed to create execution service");

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    // test query timeout
    let query_handle = execution_svc
        .submit_query(
            "test_session_id",
            "SELECT sleep(2)",
            QueryContext::default(),
        )
        .await
        .expect("Failed to submit query");

    let query_id = query_handle.query_id;

    let query_result = execution_svc
        .wait_submitted_query_result(query_handle)
        .await
        .expect_err("Query should not succeed");

    let query_result_str = format!("{query_result:?}");
    match query_result {
        Error::QueryExecution { source, .. } => match *source {
            Error::QueryTimeout { .. } => {}
            _ => panic!("Expected query status: Canceled, but got {query_result_str}"),
        },
        _ => panic!("Expected outer QueryExecution error, but got {query_result_str}"),
    }

    let query_record = history_store
        .get_query(query_id)
        .await
        .expect("Failed to get query at history store after query timeout");

    assert_eq!(query_record.query_id(), query_id);
    assert_eq!(query_record.status, QueryStatus::TimedOut);
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_submitted_query_abort_by_query_id() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore,
        history_store.clone(),
        Arc::new(Config::default().with_query_timeout(1)),
    )
    .await
    .expect("Failed to create execution service");

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    // test cancel query
    let query_handle = execution_svc
        .submit_query(
            "test_session_id",
            "SELECT sleep(2)",
            QueryContext::default(),
        )
        .await
        .expect("Failed to submit query");

    let query_id = query_handle.query_id;

    let query_status = execution_svc
        .abort_query(RunningQueryId::ByQueryId(query_id))
        .await
        .expect("Failed to cancel query");
    assert_eq!(query_status, QueryStatus::Canceled);

    let query_record = history_store
        .get_query(query_id)
        .await
        .expect("Failed to get query at history store after query timeout");

    assert_eq!(query_record.query_id(), query_id);
    assert_eq!(query_record.status, QueryStatus::Canceled);
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_submitted_query_abort_by_request_id() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore,
        history_store.clone(),
        Arc::new(Config::default().with_query_timeout(1)),
    )
    .await
    .expect("Failed to create execution service");

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    let request_id = uuid::Uuid::new_v4();
    let sql_text = "SELECT sleep(2)";
    // test cancel query
    let query_handle = execution_svc
        .submit_query(
            "test_session_id",
            sql_text,
            QueryContext::default().with_request_id(request_id),
        )
        .await
        .expect("Failed to submit query");

    let query_id = query_handle.query_id;

    let query_status =execution_svc
        .abort_query(RunningQueryId::ByRequestId(
            request_id,
            sql_text.to_string(),
        ))
        .await
        .expect("Failed to cancel query");
    assert_eq!(query_status, QueryStatus::Canceled);

    let query_record = history_store
        .get_query(query_id)
        .await
        .expect("Failed to get query at history store after query timeout");

    assert_eq!(query_record.query_id(), query_id);
    assert_eq!(query_record.status, QueryStatus::Canceled);
}

#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_submitted_query_ok() {
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    let execution_svc = CoreExecutionService::new(
        metastore,
        history_store.clone(),
        Arc::new(Config::default().with_query_timeout(2)),
    )
    .await
    .expect("Failed to create execution service");

    let _session = execution_svc
        .create_session("test_session_id")
        .await
        .expect("Failed to create session");

    let start = std::time::Instant::now();
    let query_handle = execution_svc
        .submit_query(
            "test_session_id",
            "SELECT sleep(1)",
            QueryContext::default(),
        )
        .await
        .expect("Failed to submit query");

    let duration = start.elapsed();
    eprintln!("Query submitted in {} ms", duration.as_millis());
    // check if query submitted significantly faster than it supposed to be executed
    assert!(duration < std::time::Duration::from_millis(300));

    let query_id = query_handle.query_id;

    let _query_result = execution_svc
        .wait_submitted_query_result(query_handle)
        .await
        .expect("Query should be completed successfully");

    let query_record = history_store
        .get_query(query_id)
        .await
        .expect("Failed to get query at history store after query timeout");

    assert_eq!(query_record.query_id(), query_id);
    assert_eq!(query_record.status, QueryStatus::Successful);
}
