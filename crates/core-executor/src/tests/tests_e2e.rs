use crate::models::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use core_history::store::SlateDBHistoryStore;
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::Volume as MetastoreVolume;
use core_utils::Db;
use std::sync::Arc;
use futures::future::join_all;

pub const TEST_SESSION_ID: &str = "test_session_id";
pub const TEST_VOLUME_NAME: &str = "test_volume";
pub const TEST_DATABASE_NAME: &str = "embucket";
pub const TEST_SCHEMA_NAME: &str = "public";

pub struct TestEntities {
    pub volume: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

async fn create_executor() -> CoreExecutionService {
    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(SlateDBHistoryStore::new(db));
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    );

    metastore
        .create_volume(
            &TEST_VOLUME_NAME.to_string(),
            MetastoreVolume::new(
                TEST_VOLUME_NAME.to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");

    execution_svc
}

#[tokio::test]
#[ignore]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn e2e_test_multiple_writers() {
    let execution_svc = create_executor().await;

    execution_svc.create_session(TEST_SESSION_ID.to_string()).await
        .expect("Failed to create session");

    execution_svc
        .query(TEST_SESSION_ID, &format!("CREATE DATABASE {TEST_DATABASE_NAME} EXTERNAL_VOLUME = {TEST_VOLUME_NAME}"), QueryContext::default())
        .await
        .expect("Failed to create database");

    execution_svc
        .query(TEST_SESSION_ID, &format!("CREATE SCHEMA {TEST_DATABASE_NAME}.{TEST_SCHEMA_NAME}"), QueryContext::default())
        .await
        .expect("Failed to create schema");

    let query = format!("CREATE TABLE {TEST_DATABASE_NAME}.{TEST_SCHEMA_NAME}.hello(amount number)");
    let mut futures = vec![];
    for _ in 0..5 {
        futures.push(execution_svc
            .query(TEST_SESSION_ID, &query, QueryContext::default()));
    }

    let results = join_all(futures).await;

    eprintln!("Results: {:#?}", results);
    let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
    let oks: Vec<_> = oks.into_iter().map(|r| r.unwrap()).collect();
    let errs: Vec<_> = errs.into_iter().map(|r| r.unwrap_err()).collect();
    assert_eq!(oks.len(), 1);
    assert_eq!(errs.len(), 4);
}
