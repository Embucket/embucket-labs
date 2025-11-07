mod options;
mod run;

use core_executor::models::QueryContext;
use core_executor::service::CoreExecutionService;
use core_executor::session::UserSession;
use core_executor::utils::Config;
use core_history::SlateDBHistoryStore;
use core_metastore::SlateDBMetastore;
use datafusion::error::Result;
pub use options::{BoolDefaultTrue, CommonOpt};
pub use run::{BenchQuery, BenchmarkRun};
use std::sync::Arc;

/// Default catalog name used in benchmarks
pub const DEFAULT_CATALOG: &str = "benchmark";

/// Default schema name used in benchmarks
pub const DEFAULT_SCHEMA: &str = "public";

#[must_use]
pub fn query_context() -> QueryContext {
    QueryContext::new(
        Some(DEFAULT_CATALOG.to_string()),
        Some(DEFAULT_SCHEMA.to_string()),
        None,
    )
}

#[must_use]
pub fn table_ref(table: &str) -> String {
    format!("{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table}")
}

#[allow(clippy::unwrap_used)]
pub async fn create_catalog(path: &str, session: &Arc<UserSession>, mem_table: bool) -> Result<()> {
    let volume_sql = if mem_table {
        "CREATE EXTERNAL VOLUME test STORAGE_LOCATIONS = ((NAME = 'mem_vol' STORAGE_PROVIDER = 'MEMORY'))".to_string()
    } else {
        format!(
            "CREATE EXTERNAL VOLUME test STORAGE_LOCATIONS = (\
        (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{path}/data'))"
        )
    };
    let mut volume_query = session.query(volume_sql, query_context());
    volume_query.execute().await?;

    let database_sql =
        format!("CREATE DATABASE IF NOT EXISTS {DEFAULT_CATALOG} EXTERNAL_VOLUME = test");
    let mut database_query = session.query(database_sql, query_context());
    database_query.execute().await?;

    let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}");
    let mut schema_query = session.query(schema_sql, query_context());
    schema_query.execute().await?;
    Ok(())
}

pub async fn set_session_variable_number(
    var: &str,
    value: usize,
    session: &Arc<UserSession>,
) -> Result<()> {
    let var_query = format!("SET datafusion.{var} = {value}");
    let mut query = session.query(var_query, query_context());
    query.execute().await?;
    Ok(())
}

pub async fn set_session_variable_bool(
    var: &str,
    value: bool,
    session: &Arc<UserSession>,
) -> Result<()> {
    let var_query = format!("SET datafusion.{var} = {value}");
    let mut query = session.query(var_query, query_context());
    query.execute().await?;
    Ok(())
}

#[allow(clippy::expect_used, clippy::as_conversions)]
pub async fn make_test_execution_svc() -> Arc<CoreExecutionService> {
    // let object_store = LocalFileSystem::new_with_prefix(PathBuf::from("."))
    //     .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
    //     .expect("Failed to create file object_store");
    //
    // let db = Db::new(Arc::new(
    //     DbBuilder::new(Path::from("slatedb_prefix"), object_store.clone())
    //         .build()
    //         .await
    //         .expect("Failed to start Slate DB"),
    // ));
    let metastore = Arc::new(SlateDBMetastore::new_in_memory().await);
    let history_store = Arc::new(SlateDBHistoryStore::new_in_memory().await);
    Arc::new(
        CoreExecutionService::new(metastore, history_store, Arc::new(Config::default()))
            .await
            .expect("Failed to create a execution service"),
    )
}
