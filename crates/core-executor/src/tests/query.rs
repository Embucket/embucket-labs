use crate::session::UserSession;
use std::collections::HashMap;

use crate::models::QueryContext;
use crate::running_queries::RunningQueriesRegistry;
use crate::service::CoreExecutionService;
use crate::utils::Config;
#[cfg(test)]
use core_history::MockHistoryStore;
use core_history::{HistoryStore, QueryRecord};
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume,
};
use core_utils::Db;
use datafusion::sql::parser::DFParser;
use datafusion_table_providers::duckdb::{DuckDBTableFactory, DuckDBTableProviderFactory};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use embucket_functions::session_params::SessionProperty;
use std::sync::Arc;

#[allow(clippy::unwrap_used)]
#[tokio::test]
async fn test_update_all_table_names_visitor() {
    let args = vec![
        ("select * from foo", "SELECT * FROM embucket.new_schema.foo"),
        (
            "insert into foo (id) values (5)",
            "INSERT INTO embucket.new_schema.foo (id) VALUES (5)",
        ),
        (
            "insert into foo select * from bar",
            "INSERT INTO embucket.new_schema.foo SELECT * FROM embucket.new_schema.bar",
        ),
        (
            "insert into foo select * from bar where id = 1",
            "INSERT INTO embucket.new_schema.foo SELECT * FROM embucket.new_schema.bar WHERE id = 1",
        ),
        (
            "select * from foo join bar on foo.id = bar.id",
            "SELECT * FROM embucket.new_schema.foo JOIN embucket.new_schema.bar ON foo.id = bar.id",
        ),
        (
            "select * from foo where id = 1",
            "SELECT * FROM embucket.new_schema.foo WHERE id = 1",
        ),
        (
            "select count(*) from foo",
            "SELECT count(*) FROM embucket.new_schema.foo",
        ),
        (
            "WITH sales_data AS (SELECT * FROM foo) SELECT * FROM sales_data",
            "WITH sales_data AS (SELECT * FROM embucket.new_schema.foo) SELECT * FROM sales_data",
        ),
        // Skip table functions
        (
            "select * from result_scan('1')",
            "SELECT * FROM result_scan('1')",
        ),
        (
            "SELECT * from flatten('[1,77]','',false,false,'both')",
            "SELECT * FROM flatten('[1,77]', '', false, false, 'both')",
        ),
    ];

    let session = create_df_session().await;
    let mut params = HashMap::new();
    params.insert(
        "schema".to_string(),
        SessionProperty::from_str_value("schema".to_string(), "new_schema".to_string(), None),
    );
    session.set_session_variable(true, params).unwrap();
    let query = session.query("", QueryContext::default());
    for (init, exp) in args {
        let statement = DFParser::parse_sql(init).unwrap().pop_front();
        if let Some(mut s) = statement {
            query.update_statement_references(&mut s).unwrap();
            assert_eq!(s.to_string(), exp);
        }
    }
}

static TABLE_SETUP: &str = include_str!(r"./table_setup.sql");

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn create_df_session() -> Arc<UserSession> {
    let db = Db::memory().await;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let mut mock = MockHistoryStore::new();
    mock.expect_get_queries().returning(|_| {
        let mut records = Vec::new();
        for i in 0..3 {
            let mut q = QueryRecord::new("query", None);
            q.id = i.into();
            records.push(q);
        }
        Ok(records)
    });
    let history_store: Arc<dyn HistoryStore> = Arc::new(mock);
    let running_queries = Arc::new(RunningQueriesRegistry::new());

    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"embucket".to_string(),
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
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
    let config = Arc::new(Config::default());
    let catalog_list = CoreExecutionService::catalog_list(metastore.clone(), history_store.clone())
        .await
        .expect("Failed to create catalog list");
    let runtime_env = CoreExecutionService::runtime_env(&config, catalog_list.clone())
        .expect("Failed to create runtime env");
    let duckdb_pool = Arc::new(
        DuckDbConnectionPool::new_memory().expect("unable to create DuckDB connection pool"),
    );
    let duckdb_table_factory = Arc::new(DuckDBTableFactory::new(duckdb_pool));

    let user_session = Arc::new(
        UserSession::new(
            metastore,
            history_store,
            running_queries, // queries aborting will not work, unless its properly used (as in ExecutionService)
            Arc::new(Config::default()),
            catalog_list,
            runtime_env,
            duckdb_table_factory,
        )
        .expect("Failed to create user session"),
    );

    for query in TABLE_SETUP.split(';') {
        if !query.is_empty() {
            let mut query = user_session.query(query, QueryContext::default());
            query.execute().await.unwrap();
        }
    }
    user_session
}

#[macro_export]
macro_rules! test_query {
    (
        $test_fn_name:ident,
        $query:expr
        $(, setup_queries =[$($setup_queries:expr),* $(,)?])?
        $(, sort_all = $sort_all:expr)?
        $(, exclude_columns = [$($excluded:expr),* $(,)?])?
        $(, snapshot_path = $user_snapshot_path:expr)?
        $(, snowflake_error = $snowflake_error:expr)?
    ) => {
        paste::paste! {
            #[tokio::test]
            async fn [< query_ $test_fn_name >]() {
                let ctx = $crate::tests::query::create_df_session().await;

                // Execute all setup queries (if provided) to set up the session context
                $(
                    $(
                        {
                            let mut q = ctx.query($setup_queries, $crate::models::QueryContext::default());
                            q.execute().await.unwrap();
                        }
                    )*
                )?

                let mut query = ctx.query($query, $crate::models::QueryContext::default().with_ip_address("test_ip".to_string()));
                let res = query.execute().await;
                let sort_all = false $(|| $sort_all)?;
                let excluded_columns: std::collections::HashSet<&str> = std::collections::HashSet::from([
                    $($($excluded),*)?
                ]);
                let snowflake_error = false $(|| $snowflake_error)?;
                let mut settings = insta::Settings::new();
                settings.set_description(stringify!($query));
                settings.set_omit_expression(true);
                settings.set_prepend_module_to_snapshot(false);
                settings.set_snapshot_path(concat!("snapshots", "/") $(.to_owned() + $user_snapshot_path)?);

                let setup: Vec<&str> = vec![$($($setup_queries),*)?];
                if !setup.is_empty() {
                    settings.set_info(
                        &format!(
                            "{}Setup queries: {}",
                            if snowflake_error { "Tests Snowflake Error; " } else { "" },
                            setup.join("; "),
                        ),
                    );
                } else if snowflake_error {
                    settings.set_info(&format!("Tests Snowflake Error"));
                }
                settings.bind(|| {
                    let df = match res {
                        Ok(record_batches) => {
                            let mut batches: Vec<datafusion::arrow::array::RecordBatch> = record_batches.records;
                            if !excluded_columns.is_empty() {
                                batches = df_catalog::test_utils::remove_columns_from_batches(batches, &excluded_columns);
                            }

                            if sort_all {
                                for batch in &mut batches {
                                    *batch = df_catalog::test_utils::sort_record_batch_by_sortable_columns(batch);
                                }
                            }
                            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string())
                        },
                        Err(e) => {
                            if snowflake_error {
                                // Do not convert to QueryExecution error before turning to snowflake error
                                // since we don't need query_id here
                                let e = e.to_snowflake_error();

                                // location is only available for debug purposes for not handled errors.
                                // it should not be saved to the snapshot, if location bothers you then
                                // remove snowflake_error macros arg or set to false.
                                let mut location = e.unhandled_location();
                                if !location.is_empty() {
                                    location = format!("; location: {}", location);
                                }

                                Err(format!("Snowflake Error: {e}{location}"))
                            } else {
                                Err(format!("Error: {e}"))
                            }
                        }
                    };

                    let df = df.map(|df| df.split('\n').map(|s| s.to_string()).collect::<Vec<String>>());
                    insta::assert_debug_snapshot!((df));
                });
            }
        }
    };
}

// SELECT
test_query!(select_date_add_diff, "SELECT dateadd(day, 5, '2025-06-01')");
test_query!(func_date_add, "SELECT date_add(day, 30, '2025-01-06')");
test_query!(select_star, "SELECT * FROM employee_table");
// FIXME: ILIKE is not supported yet
// test_query!(select_ilike, "SELECT * ILIKE '%id%' FROM employee_table;");
test_query!(
    select_exclude,
    "SELECT * EXCLUDE department_id FROM employee_table;"
);
test_query!(
    select_exclude_multiple,
    "SELECT * EXCLUDE (department_id, employee_id) FROM employee_table;"
);

test_query!(
    qualify,
    "SELECT product_id, retail_price, quantity, city
    FROM sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY retail_price) = 1;"
);

// DESCRIBE TABLE
test_query!(describe_table, "DESCRIBE TABLE employee_table");

// SESSION RELATED https://docs.snowflake.com/en/sql-reference/commands-session
test_query!(
    alter_session_set,
    "SHOW VARIABLES",
    setup_queries = ["ALTER SESSION SET v1 = 'test'"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "session"
);
test_query!(
    alter_session_unset,
    "SHOW VARIABLES",
    setup_queries = [
        "ALTER SESSION SET v1 = 'test' v2 = 1",
        "ALTER SESSION UNSET v1"
    ],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "session"
);

test_query!(
    set_variable_with_binary_op_placeholder,
    "SELECT $max",
    setup_queries = [
        "SET (min, max) = (40, 70);",
        "SET (min, max) = (50, 2 * $min)",
    ],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "session"
);
test_query!(
    set_variable_and_access_by_placeholder,
    "SELECT $v1",
    setup_queries = ["SET v1 = 'test';"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "session"
);
test_query!(
    set_variable_system,
    "SELECT name, value FROM snowplow.information_schema.df_settings
     WHERE name = 'datafusion.execution.time_zone'",
    setup_queries = ["SET datafusion.execution.time_zone = 'TEST_TIMEZONE'"],
    snapshot_path = "session"
);
// TODO Currently UNSET is not supported
test_query!(
    unset_variable,
    "UNSET v3",
    setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    snapshot_path = "session"
);
test_query!(
    session_last_query_id,
    "SELECT
        length(LAST_QUERY_ID()) > 0 as last,
        length(LAST_QUERY_ID(-1)) > 0 as last_index,
        length(LAST_QUERY_ID(2)) > 0 as second,
        length(LAST_QUERY_ID(100)) = 0 as empty",
    setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    snapshot_path = "session"
);

// https://docs.snowflake.com/en/sql-reference/sql/explain
// https://datafusion.apache.org/user-guide/sql/explain.html
// Datafusion has different output format.
// Check session config ExplainOptions for the full list of options
// logical_only_plan flag is used to only print logical plans
// since physical plan contains dynamic files names
test_query!(
    explain_select,
    "EXPLAIN SELECT * FROM embucket.public.employee_table",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_limit,
    "EXPLAIN SELECT * FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_column,
    "EXPLAIN SELECT last_name FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_missing_column,
    "EXPLAIN SELECT missing FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
// Session context
test_query!(
    session_objects,
    "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
    snapshot_path = "session"
);
test_query!(
    session_objects_with_aliases,
    "SELECT CURRENT_WAREHOUSE() as wh, CURRENT_DATABASE() as db, CURRENT_SCHEMA() as sch",
    snapshot_path = "session"
);
test_query!(
    session_current_schemas,
    "SELECT CURRENT_SCHEMAS()",
    snapshot_path = "session"
);
test_query!(
    session_current_schemas_with_aliases,
    "SELECT CURRENT_SCHEMAS() as sc",
    snapshot_path = "session"
);
test_query!(
    session_general,
    "SELECT CURRENT_VERSION(), CURRENT_CLIENT()",
    snapshot_path = "session"
);
test_query!(
    session,
    "SELECT CURRENT_ROLE_TYPE(), CURRENT_ROLE()",
    snapshot_path = "session"
);
test_query!(
    session_current_session,
    // Check only length of session id since it is dynamic uuid
    "SELECT length(CURRENT_SESSION())",
    snapshot_path = "session"
);
test_query!(
    session_current_ip_address,
    "SELECT CURRENT_IP_ADDRESS()",
    snapshot_path = "session"
);

test_query!(
    merge_into_only_update,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row')",
        "INSERT INTO embucket.public.merge_source VALUES (1, 'updated row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET merge_target.description = merge_source.description",
    ]
);

test_query!(
    merge_into_insert_and_update,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "INSERT INTO embucket.public.merge_source VALUES (2, 'updated row'), (3, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

test_query!(
    merge_into_empty_source,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

test_query!(
    merge_into_ctas_source_multi_insert_target,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row'), (3, 'existing row'), (4, 'existing row'), (5, 'existing row')",
        "INSERT INTO embucket.public.merge_target VALUES (6, 'existing row'), (7, 'existing row'), (8, 'existing row'), (9, 'existing row')",
        "INSERT INTO embucket.public.merge_target VALUES (11, 'existing row'), (12, 'existing row'), (13, 'existing row'), (14, 'existing row'), (15, 'existing row')",
        "INSERT INTO embucket.public.merge_target VALUES (16, 'existing row'), (17, 'existing row'), (18, 'existing row'), (19, 'existing row')",
        "CREATE OR REPLACE TABLE embucket.public.merge_source AS SELECT column1 as id, column2 as description FROM VALUES (1, 'updated row'), (10, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
        "CREATE OR REPLACE TABLE embucket.public.merge_source AS SELECT column1 as id, column2 as description FROM VALUES (11, 'updated row'), (20, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

test_query!(
    merge_into_ambigious_insert,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "INSERT INTO embucket.public.merge_source VALUES (2, 'updated row'), (3, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (id, merge_source.description)",
    ]
);

test_query!(
    merge_into_insert_and_update_alias,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "INSERT INTO embucket.public.merge_source VALUES (2, 'updated row'), (3, 'new row')",
        "MERGE INTO merge_target t USING merge_source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET description = s.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (s.id, s.description)",
    ]
);

test_query!(
    merge_into_with_predicate,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "INSERT INTO embucket.public.merge_source VALUES (2, 'updated row'), (3, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED AND merge_target.id = 1 THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

test_query!(
    merge_into_with_values,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "MERGE INTO merge_target USING (SELECT * FROM (VALUES (2, 'updated row'), (3, 'new row')) AS source(id, description)) AS source ON merge_target.id = source.id WHEN MATCHED THEN UPDATE SET description = source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (source.id, source.description)",
    ]
);

test_query!(
    merge_into_empty_table,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_source VALUES (2, 'updated row'), (3, 'new row')",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

test_query!(
    merge_into_from_view,
    "SELECT count(CASE WHEN description = 'updated row' THEN 1 ELSE NULL END) updated, count(CASE WHEN description = 'existing row' THEN 1 ELSE NULL END) existing FROM embucket.public.merge_target",
    setup_queries = [
        "CREATE TABLE embucket.public.merge_target (ID INTEGER, description VARCHAR)",
        "CREATE TABLE embucket.public.merge_source_table (ID INTEGER, description VARCHAR)",
        "INSERT INTO embucket.public.merge_target VALUES (1, 'existing row'), (2, 'existing row')",
        "INSERT INTO embucket.public.merge_source_table VALUES (2, 'updated row'), (3, 'new row')",
        "CREATE VIEW embucket.public.merge_source AS SELECT * FROM embucket.public.merge_source_table",
        "MERGE INTO merge_target USING merge_source ON merge_target.id = merge_source.id WHEN MATCHED THEN UPDATE SET description = merge_source.description WHEN NOT MATCHED THEN INSERT (id, description) VALUES (merge_source.id, merge_source.description)",
    ]
);

// TRUNCATE TABLE
test_query!(truncate_table, "TRUNCATE TABLE employee_table");
test_query!(
    truncate_table_full,
    "TRUNCATE TABLE embucket.public.employee_table"
);
test_query!(
    truncate_table_full_quotes,
    "TRUNCATE TABLE 'EMBUCKET'.'PUBLIC'.'EMPLOYEE_TABLE'"
);
test_query!(truncate_missing, "TRUNCATE TABLE missing_table");

test_query!(
    merge_into_column_only_optimization,
    "SELECT * FROM column_only_optimization_target ORDER BY a,b",
    setup_queries = [
        "CREATE TABLE column_only_optimization_target(a int,b string)",
        "CREATE TABLE column_only_optimization_source(a int,b string)",
        "INSERT INTO column_only_optimization_target VALUES(1,'a1'),(2,'a2')",
        "INSERT INTO column_only_optimization_target VALUES(3,'a3'),(4,'a4')",
        "INSERT INTO column_only_optimization_target VALUES(5,'a5'),(6,'a6')",
        "INSERT INTO column_only_optimization_target VALUES(7,'a7'),(8,'a8')",
        "INSERT INTO column_only_optimization_source VALUES(1,'b1'),(2,'b2')",
        "INSERT INTO column_only_optimization_source VALUES(3,'b3'),(4,'b4')",
        "MERGE INTO column_only_optimization_target AS t1 USING column_only_optimization_source AS t2 ON t1.a = t2.a WHEN MATCHED THEN UPDATE SET t1.b = t2.b WHEN NOT MATCHED THEN INSERT (a,b) VALUES (t2.a, t2.b)",
    ]
);

test_query!(
    merge_into_without_distributed_enable,
    "SELECT * FROM t1 ORDER BY a,b,c",
    setup_queries = [
        "CREATE OR REPLACE TABLE t1(a int,b string, c string)",
        "CREATE OR REPLACE TABLE t2(a int,b string, c string)",
        "INSERT INTO t1 VALUES(1,'b1','c1'),(2,'b2','c2')",
        "INSERT INTO t1 VALUES(2,'b3','c3'),(3,'b4','c4')",
        "INSERT INTO t2 VALUES(1,'b_5','c_5'),(3,'b_6','c_6')",
        "INSERT INTO t2 VALUES(2,'b_7','c_7')",
        "MERGE INTO t1 USING (SELECT * FROM t2) AS t2 ON t1.a = t2.a WHEN MATCHED THEN UPDATE SET t1.c = t2.c",
        "INSERT INTO t2 VALUES(4,'b_8','c_8')",
        "MERGE INTO t1 USING (SELECT * FROM t2) AS t2 ON t1.a = t2.a WHEN MATCHED THEN UPDATE SET t1.c = t2.c WHEN NOT MATCHED THEN INSERT (a,b,c) VALUES(t2.a,t2.b,t2.c)",
    ]
);

test_query!(
    merge_into_with_partial_insert,
    "SELECT * FROM t1 ORDER BY a,b,c",
    setup_queries = [
        "CREATE OR REPLACE TABLE t1(a int,b string, c string)",
        "CREATE OR REPLACE TABLE t2(a int,b string, c string)",
        "INSERT INTO t1 VALUES(1,'b1','c1'),(2,'b2','c2')",
        "INSERT INTO t1 VALUES(2,'b3','c3'),(3,'b4','c4')",
        "INSERT INTO t2 VALUES(1,'b_5','c_5'),(3,'b_6','c_6')",
        "INSERT INTO t2 VALUES(2,'b_7','c_7')",
        "INSERT INTO t2 VALUES(4,'b_8','c_8')",
        "MERGE INTO t1 USING (SELECT * FROM t2) AS t2 ON t1.a = t2.a WHEN MATCHED THEN UPDATE SET t1.c = t2.c WHEN NOT MATCHED THEN INSERT (a,c) VALUES(t2.a,t2.c)",
    ]
);

test_query!(
    copy_into_without_volume,
    "SELECT SUM(L_QUANTITY) FROM embucket.public.lineitem;",
    setup_queries = [
        "CREATE TABLE embucket.public.lineitem (
    L_ORDERKEY BIGINT NOT NULL,
    L_PARTKEY BIGINT NOT NULL,
    L_SUPPKEY BIGINT NOT NULL,
    L_LINENUMBER INT NOT NULL,
    L_QUANTITY DOUBLE NOT NULL,
    L_EXTENDED_PRICE DOUBLE NOT NULL,
    L_DISCOUNT DOUBLE NOT NULL,
    L_TAX DOUBLE NOT NULL,
    L_RETURNFLAG CHAR NOT NULL,
    L_LINESTATUS CHAR NOT NULL,
    L_SHIPDATE DATE NOT NULL,
    L_COMMITDATE DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT VARCHAR NOT NULL,
    L_SHIPMODE VARCHAR NOT NULL,
    L_COMMENT VARCHAR NOT NULL );",
        "COPY INTO embucket.public.lineitem FROM 's3://embucket-testdata/tpch/lineitem.csv' FILE_FORMAT = ( TYPE = CSV );"
    ]
);

test_query!(
    timestamp_str_format,
    "SELECT
       TO_TIMESTAMP('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss') as a,
       TO_TIMESTAMP('04/05/2024 01:02:03') as b",
    setup_queries = ["SET timestamp_input_format = 'mm/dd/yyyy hh24:mi:ss'"]
);

test_query!(
    timestamp_timezone,
    "SELECT TO_TIMESTAMP(1000000000)",
    setup_queries = ["ALTER SESSION SET timestamp_input_mapping = 'timestamp_tz'"]
);

test_query!(
    timestamp_with_timezone_to_timestamp,
    "SELECT TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', '2024-12-31 10:00:00.000'::TIMESTAMP)) as model_tstamp;"
);

// Basic date part extraction tests
test_query!(
    date_part_extract_basic,
    r#"SELECT '2016-01-02T23:39:20.123-07:00'::TIMESTAMP AS tstamp,
        YEAR('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "YEAR",
        QUARTER('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "QUARTER",
        MONTH('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "MONTH",
        DAY('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "DAY",
        DAYOFMONTH('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF MONTH",
        DAYOFWEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF WEEK",
        DAYOFWEEKISO('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF WEEK ISO",
        DAYOFYEAR('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "DAY OF YEAR",
        HOUR('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "HOUR",
        MINUTE('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "MINUTE",
        SECOND('2016-01-02T23:39:20.123-07:00'::TIMESTAMP) AS "SECOND""#,
    snapshot_path = "date_part_extract"
);

test_query!(
    date_part_extract_week_of_year_policy,
    "SELECT WEEK('2016-01-02T23:39:20.123-07:00'::TIMESTAMP)",
    setup_queries = ["ALTER SESSION SET WEEK_OF_YEAR_POLICY = '1'"],
    snapshot_path = "date_part_extract"
);

// Basic timezone conversion tests
test_query!(
    convert_timezone_basic_utc_to_est,
    r"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00Z'::TIMESTAMP) AS est_time",
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_basic_utc_to_pst,
    r"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/Los_Angeles', '2024-01-15T12:00:00Z'::TIMESTAMP) AS pst_time",
    snapshot_path = "convert_timezone"
);

// Test with explicit source timezone (3-argument form)
test_query!(
    convert_timezone_explicit_source,
    r"SELECT
        '2024-01-15T12:00:00'::TIMESTAMP AS source_time,
        CONVERT_TIMEZONE('America/New_York', 'America/Los_Angeles', '2024-01-15T12:00:00'::TIMESTAMP) AS converted_time",
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_utc_to_multiple_zones,
    r"SELECT
        '2024-06-15T15:30:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/New_York', '2024-06-15T15:30:00Z'::TIMESTAMP) AS new_york,
        CONVERT_TIMEZONE('America/Los_Angeles', '2024-06-15T15:30:00Z'::TIMESTAMP) AS los_angeles,
        CONVERT_TIMEZONE('Europe/London', '2024-06-15T15:30:00Z'::TIMESTAMP) AS london,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-06-15T15:30:00Z'::TIMESTAMP) AS tokyo",
    snapshot_path = "convert_timezone"
);

// Test daylight saving time transitions
test_query!(
    convert_timezone_dst_spring_forward,
    r"-- Spring forward: 2024-03-10 02:00 AM EST becomes 03:00 AM EDT
    WITH dst_times AS (
        SELECT '2024-03-10T06:00:00Z'::TIMESTAMP AS utc_before  -- 1 AM EST
        UNION ALL SELECT '2024-03-10T07:00:00Z'::TIMESTAMP      -- 3 AM EDT (2 AM skipped)
        UNION ALL SELECT '2024-03-10T08:00:00Z'::TIMESTAMP      -- 4 AM EDT
    )
    SELECT
        utc_before AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_before) AS eastern_time
    FROM dst_times
    ORDER BY utc_before",
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_dst_fall_back,
    r"-- Fall back: 2024-11-03 02:00 AM EDT becomes 01:00 AM EST
    WITH dst_times AS (
        SELECT '2024-11-03T05:00:00Z'::TIMESTAMP AS utc_before  -- 1 AM EDT
        UNION ALL SELECT '2024-11-03T06:00:00Z'::TIMESTAMP      -- 1 AM EST (repeated hour)
        UNION ALL SELECT '2024-11-03T07:00:00Z'::TIMESTAMP      -- 2 AM EST
    )
    SELECT
        utc_before AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_before) AS eastern_time
    FROM dst_times
    ORDER BY utc_before",
    snapshot_path = "convert_timezone"
);

// Test different timestamp precisions
test_query!(
    convert_timezone_different_precisions,
    r"SELECT
        '2024-01-15T12:00:00'::TIMESTAMP AS seconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00'::TIMESTAMP) AS converted_seconds,
        '2024-01-15T12:00:00.123'::TIMESTAMP AS milliseconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00.123'::TIMESTAMP) AS converted_milliseconds,
        '2024-01-15T12:00:00.123456'::TIMESTAMP AS microseconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00.123456'::TIMESTAMP) AS converted_microseconds",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions between non-UTC zones
test_query!(
    convert_timezone_non_utc_conversions,
    r"SELECT
        '2024-07-15T14:30:00'::TIMESTAMP AS source_time,
        CONVERT_TIMEZONE('America/New_York', 'Europe/London', '2024-07-15T14:30:00'::TIMESTAMP) AS ny_to_london,
        CONVERT_TIMEZONE('Europe/London', 'Asia/Tokyo', '2024-07-15T14:30:00'::TIMESTAMP) AS london_to_tokyo,
        CONVERT_TIMEZONE('Asia/Tokyo', 'America/Los_Angeles', '2024-07-15T14:30:00'::TIMESTAMP) AS tokyo_to_la",
    snapshot_path = "convert_timezone"
);

// Test with table data
test_query!(
    convert_timezone_table_data,
    r"WITH event_times AS (
        SELECT 'Meeting Start' AS event, '2024-01-15T09:00:00Z'::TIMESTAMP AS utc_time
        UNION ALL SELECT 'Lunch Break', '2024-01-15T12:00:00Z'::TIMESTAMP
        UNION ALL SELECT 'Meeting End', '2024-01-15T17:00:00Z'::TIMESTAMP
        UNION ALL SELECT 'Dinner', '2024-01-15T19:30:00Z'::TIMESTAMP
    )
    SELECT
        event,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern_time,
        CONVERT_TIMEZONE('America/Los_Angeles', utc_time) AS pacific_time,
        CONVERT_TIMEZONE('Europe/London', utc_time) AS london_time
    FROM event_times
    ORDER BY utc_time",
    snapshot_path = "convert_timezone"
);

// Test edge cases around midnight
test_query!(
    convert_timezone_midnight_edge_cases,
    r"WITH midnight_times AS (
        SELECT '2024-01-15T00:00:00Z'::TIMESTAMP AS utc_midnight
        UNION ALL SELECT '2024-01-15T23:59:59Z'::TIMESTAMP AS utc_almost_midnight
        UNION ALL SELECT '2024-01-16T00:00:00Z'::TIMESTAMP AS utc_next_day
    )
    SELECT
        utc_midnight AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_midnight) AS eastern_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_midnight) AS tokyo_time
    FROM midnight_times
    ORDER BY utc_midnight",
    snapshot_path = "convert_timezone"
);

// Test year boundary conversions
test_query!(
    convert_timezone_year_boundary,
    r"WITH year_boundary AS (
        SELECT '2023-12-31T23:00:00Z'::TIMESTAMP AS utc_time, 'New Year Eve' AS description
        UNION ALL SELECT '2024-01-01T00:00:00Z'::TIMESTAMP, 'New Year UTC'
        UNION ALL SELECT '2024-01-01T05:00:00Z'::TIMESTAMP, 'New Year EST'
    )
    SELECT
        description,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_time) AS tokyo_time
    FROM year_boundary
    ORDER BY utc_time",
    snapshot_path = "convert_timezone"
);

// Test common business timezone conversions
test_query!(
    convert_timezone_business_hours,
    r"-- Business hours conversion: 9 AM EST to various timezones
    WITH business_hours AS (
        SELECT '2024-03-15T14:00:00Z'::TIMESTAMP AS utc_time, '9 AM EST' AS description
        UNION ALL SELECT '2024-03-15T17:00:00Z'::TIMESTAMP, '12 PM EST'
        UNION ALL SELECT '2024-03-15T22:00:00Z'::TIMESTAMP, '5 PM EST'
    )
    SELECT
        description,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern,
        CONVERT_TIMEZONE('America/Chicago', utc_time) AS central,
        CONVERT_TIMEZONE('America/Denver', utc_time) AS mountain,
        CONVERT_TIMEZONE('America/Los_Angeles', utc_time) AS pacific
    FROM business_hours
    ORDER BY utc_time",
    snapshot_path = "convert_timezone"
);

// Test international timezone conversions
test_query!(
    convert_timezone_international,
    r"SELECT
        '2024-06-15T12:00:00Z'::TIMESTAMP AS utc_noon,
        CONVERT_TIMEZONE('Europe/London', '2024-06-15T12:00:00Z'::TIMESTAMP) AS london,
        CONVERT_TIMEZONE('Europe/Paris', '2024-06-15T12:00:00Z'::TIMESTAMP) AS paris,
        CONVERT_TIMEZONE('Europe/Berlin', '2024-06-15T12:00:00Z'::TIMESTAMP) AS berlin,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-06-15T12:00:00Z'::TIMESTAMP) AS tokyo,
        CONVERT_TIMEZONE('Asia/Shanghai', '2024-06-15T12:00:00Z'::TIMESTAMP) AS shanghai,
        CONVERT_TIMEZONE('Australia/Sydney', '2024-06-15T12:00:00Z'::TIMESTAMP) AS sydney",
    snapshot_path = "convert_timezone"
);

// Test roundtrip conversions
test_query!(
    convert_timezone_roundtrip,
    r"WITH original_time AS (
        SELECT '2024-01-15T12:00:00'::TIMESTAMP AS source_time
    )
    SELECT
        source_time,
        CONVERT_TIMEZONE('UTC', 'America/New_York', source_time) AS utc_to_est,
        CONVERT_TIMEZONE('America/New_York', 'UTC',
            CONVERT_TIMEZONE('UTC', 'America/New_York', source_time)) AS roundtrip_utc
    FROM original_time",
    snapshot_path = "convert_timezone"
);

// Test extreme timezone offsets
test_query!(
    convert_timezone_extreme_offsets,
    r"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('Pacific/Kiritimati', '2024-01-15T12:00:00Z'::TIMESTAMP) AS plus_14_hours,
        CONVERT_TIMEZONE('Pacific/Niue', '2024-01-15T12:00:00Z'::TIMESTAMP) AS minus_11_hours,
        CONVERT_TIMEZONE('Pacific/Chatham', '2024-01-15T12:00:00Z'::TIMESTAMP) AS plus_12_45_minutes",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions during different seasons
test_query!(
    convert_timezone_seasonal_differences,
    r"WITH seasonal_times AS (
        SELECT '2024-01-15T12:00:00Z'::TIMESTAMP AS winter_time, 'Winter' AS season
        UNION ALL SELECT '2024-04-15T12:00:00Z'::TIMESTAMP, 'Spring'
        UNION ALL SELECT '2024-07-15T12:00:00Z'::TIMESTAMP, 'Summer'
        UNION ALL SELECT '2024-10-15T12:00:00Z'::TIMESTAMP, 'Fall'
    )
    SELECT
        season,
        winter_time AS utc_time,
        CONVERT_TIMEZONE('America/New_York', winter_time) AS eastern_time,
        CONVERT_TIMEZONE('Europe/London', winter_time) AS london_time,
        CONVERT_TIMEZONE('Australia/Sydney', winter_time) AS sydney_time
    FROM seasonal_times
    ORDER BY winter_time",
    snapshot_path = "convert_timezone"
);

// Test historical timezone conversions
test_query!(
    convert_timezone_historical_dates,
    r"WITH historical_dates AS (
        SELECT '1970-01-01T00:00:00Z'::TIMESTAMP AS unix_epoch, 'Unix Epoch' AS description
        UNION ALL SELECT '1969-07-20T20:17:00Z'::TIMESTAMP, 'Moon Landing'
        UNION ALL SELECT '2000-01-01T00:00:00Z'::TIMESTAMP, 'Y2K'
        UNION ALL SELECT '2001-09-11T08:46:00Z'::TIMESTAMP, '9/11 First Impact'
    )
    SELECT
        description,
        unix_epoch AS utc_time,
        CONVERT_TIMEZONE('America/New_York', unix_epoch) AS eastern_time,
        CONVERT_TIMEZONE('America/Los_Angeles', unix_epoch) AS pacific_time
    FROM historical_dates
    ORDER BY unix_epoch",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions for financial markets
test_query!(
    convert_timezone_financial_markets,
    r"-- Financial market opening times in UTC converted to local times
    WITH market_opens AS (
        SELECT '2024-03-15T14:30:00Z'::TIMESTAMP AS utc_time, 'NYSE Open (9:30 AM EST)' AS market
        UNION ALL SELECT '2024-03-15T08:00:00Z'::TIMESTAMP, 'LSE Open (8:00 AM GMT)'
        UNION ALL SELECT '2024-03-15T00:00:00Z'::TIMESTAMP, 'TSE Open (9:00 AM JST)'
        UNION ALL SELECT '2024-03-15T22:00:00Z'::TIMESTAMP, 'ASX Open (9:00 AM AEDT)'
    )
    SELECT
        market,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS new_york_time,
        CONVERT_TIMEZONE('Europe/London', utc_time) AS london_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_time) AS tokyo_time,
        CONVERT_TIMEZONE('Australia/Sydney', utc_time) AS sydney_time
    FROM market_opens
    ORDER BY utc_time",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions with microsecond precision
test_query!(
    convert_timezone_microsecond_precision,
    r"SELECT
        '2024-01-15T12:30:45.123456Z'::TIMESTAMP AS utc_microseconds,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS est_microseconds,
        CONVERT_TIMEZONE('Europe/Berlin', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS berlin_microseconds,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS tokyo_microseconds",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions during leap year
test_query!(
    convert_timezone_leap_year,
    r"WITH leap_year_dates AS (
        SELECT '2024-02-28T12:00:00Z'::TIMESTAMP AS feb_28, 'Feb 28 (leap year)' AS description
        UNION ALL SELECT '2024-02-29T12:00:00Z'::TIMESTAMP, 'Feb 29 (leap day)'
        UNION ALL SELECT '2024-03-01T12:00:00Z'::TIMESTAMP, 'Mar 1 (after leap day)'
        UNION ALL SELECT '2023-02-28T12:00:00Z'::TIMESTAMP, 'Feb 28 (non-leap year)'
    )
    SELECT
        description,
        feb_28 AS utc_time,
        CONVERT_TIMEZONE('America/New_York', feb_28) AS eastern_time,
        CONVERT_TIMEZONE('Europe/Paris', feb_28) AS paris_time
    FROM leap_year_dates
    ORDER BY feb_28",
    snapshot_path = "convert_timezone"
);

// Test timezone conversions with batch processing
test_query!(
    convert_timezone_batch_processing,
    r"-- Simulate batch processing of timestamps
    WITH event_log AS (
        SELECT 1 AS event_id, '2024-01-15T08:00:00Z'::TIMESTAMP AS event_time, 'System Start' AS event_type
        UNION ALL SELECT 2, '2024-01-15T12:30:00Z'::TIMESTAMP, 'User Login'
        UNION ALL SELECT 3, '2024-01-15T14:15:00Z'::TIMESTAMP, 'Data Processing'
        UNION ALL SELECT 4, '2024-01-15T16:45:00Z'::TIMESTAMP, 'Report Generation'
        UNION ALL SELECT 5, '2024-01-15T20:00:00Z'::TIMESTAMP, 'System Backup'
        UNION ALL SELECT 6, '2024-01-15T23:30:00Z'::TIMESTAMP, 'Maintenance Window'
    )
    SELECT
        event_id,
        event_type,
        event_time AS utc_time,
        CONVERT_TIMEZONE('America/New_York', event_time) AS local_time_ny,
        CONVERT_TIMEZONE('Europe/London', event_time) AS local_time_london,
        CONVERT_TIMEZONE('Asia/Singapore', event_time) AS local_time_singapore
    FROM event_log
    ORDER BY event_id",
    snapshot_path = "convert_timezone"
);
