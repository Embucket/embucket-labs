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
use object_store::local::LocalFileSystem;
use slatedb::{Db as SlateDb, config::DbOptions};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub const TEST_SESSION_ID: &str = "test_session_id";
pub const TEST_VOLUME_NAME: &str = "test_volume";
pub const TEST_DATABASE_NAME: &str = "embucket";
pub const TEST_SCHEMA_NAME: &str = "public";

#[derive(Debug)]
pub enum StorageType {
    Memory,
    File(String),
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

pub async fn create_executor(storage_type: &StorageType) -> CoreExecutionService {
    let db = match storage_type {
        StorageType::Memory => Db::memory().await,
        StorageType::File(slatedb_suffix) => {
            let mut temp_dir = env::temp_dir();
            temp_dir.push("object_store");
            let object_store = object_store(temp_dir.as_path());
            let slatedb_dir = format!("slatedb_{slatedb_suffix}");
            eprintln!("SlateDB path: {}/{}", temp_dir.display(), slatedb_dir);
            Db::new(Arc::new(
                SlateDb::open_with_opts(
                    object_store::path::Path::from(slatedb_dir),
                    DbOptions::default(),
                    object_store,
                )
                .await
                .expect("Failed to start Slate DB"),
            ))
        }
    };
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    );

    // TODO: Move po prepare_statements after we can create volume with SQL
    // Now, just ignore volume creating error, as we create multiple executors
    let _ = metastore
        .create_volume(
            &TEST_VOLUME_NAME.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_NAME.to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await;
    //.expect("Failed to create volume");

    execution_svc
        .create_session(TEST_SESSION_ID.to_string())
        .await
        .expect("Failed to create session");

    execution_svc
}

async fn exec_statements_with_multiple_hard_writers(
    n_writers: usize,
    storage_type: StorageType,
    prepare_statements: Vec<String>,
    test_statements: Vec<String>,
) -> bool {
    assert!(n_writers > 1);

    let mut execution_svc_writers: Vec<CoreExecutionService> = vec![];
    for _ in 0..n_writers {
        execution_svc_writers.push(create_executor(&storage_type).await);
    }

    // Use single writer for prepare statements
    for (idx, statement) in prepare_statements.iter().enumerate() {
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
            eprintln!("FAIL Test statement #{idx} ({statement}) Ok: {oks:#?}, Err: {errs:#?}");
            passed = false;
        } else {
            eprintln!("PASSED Test statement #{idx} ({statement})");
        }
    }
    passed
}

// async fn exec_statements_with_multiple_soft_writers(execution_svc: CoreExecutionService, n_writers: usize,
//     prepare_statements: Vec<String>, test_statements: Vec<String>) {
//     for (idx, statement) in prepare_statements.iter().enumerate() {
//         if let Err(err) = execution_svc.query(TEST_SESSION_ID, &statement, QueryContext::default()).await {
//             panic!("Prepare sql statement #{} execution error: {}", idx, err);
//         }
//     }

//     for (idx, statement) in test_statements.iter().enumerate() {
//         let mut futures = vec![];
//         for _ in 0..n_writers {
//             futures.push(execution_svc
//                 .query(TEST_SESSION_ID, &statement, QueryContext::default()));
//         }

//         let results = join_all(futures).await;

//         let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
//         let oks: Vec<_> = oks.into_iter().map(|r| r.unwrap()).collect();
//         let errs: Vec<_> = errs.into_iter().map(|r| r.unwrap_err()).collect();
//         if oks.len() != 1 || errs.len() != n_writers-1 {
//             eprintln!("FAIL Test statement #{idx} ({statement}) Ok: {oks:#?}, Err: {errs:#?}");
//         } else {
//             eprintln!("PASSED Test statement #{idx} ({statement})");
//         }
//         assert_eq!(oks.len(), 1);
//         assert_eq!(errs.len(), n_writers-1);
//     }

// }

#[tokio::test]
#[ignore = "e2e test"]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_with_multiple_writers() {
    if let Some(nano_timestamp) = Utc::now().timestamp_nanos_opt() {
        let common_slatedb_prefix = nano_timestamp.to_string();
        let storages = vec![
            StorageType::Memory,
            StorageType::File(common_slatedb_prefix),
        ];
        let mut passed = true;
        for storage in storages {
            eprintln!("Testing with storage: {storage:?}");
            passed = exec_statements_with_multiple_hard_writers(5, storage, vec![
                format!("CREATE DATABASE {TEST_DATABASE_NAME} EXTERNAL_VOLUME = {TEST_VOLUME_NAME}"),
                format!("CREATE SCHEMA {TEST_DATABASE_NAME}.{TEST_SCHEMA_NAME}"),
            ], vec![
                format!("CREATE TABLE {TEST_DATABASE_NAME}.{TEST_SCHEMA_NAME}.hello(amount number)"),
                format!("ALTER TABLE {TEST_DATABASE_NAME}.{TEST_SCHEMA_NAME}.hello add column c5 VARCHAR"),
            ]).await && passed;
        }
        assert!(passed);
    }

    // let query = ;
    // let mut futures = vec![];
    // for _ in 0..5 {
    //     futures.push(execution_svc
    //         .query(TEST_SESSION_ID, &query, QueryContext::default()));
    // }

    // let results = join_all(futures).await;

    // eprintln!("Results: {:#?}", results);
    // let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
    // let oks: Vec<_> = oks.into_iter().map(|r| r.unwrap()).collect();
    // let errs: Vec<_> = errs.into_iter().map(|r| r.unwrap_err()).collect();
    // assert_eq!(oks.len(), 1);
    // assert_eq!(errs.len(), 4);
}
