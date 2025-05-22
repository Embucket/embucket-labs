use crate::query::{QueryContext, UserQuery};
use crate::session::{SessionProperty, UserSession};

// Updated imports for new error handling
use crate::error::{
    ApplicationSnafu, ExecutionError, ExecutionResult, MetastoreSnafu, // Assuming ApplicationSnafu is for internal logic like session creation
};
use embucket_errors::wrap_error; // Import wrap_error

use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume, MetastoreResult as CoreMetastoreResult, // For mapping metastore errors
};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
// Removed unused imports for brevity: visit_expressions, Expr, ObjectName, ObjectNamePart, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, SetExpr, Value
use snafu::ResultExt; // For .context()
use std::ops::ControlFlow;
use std::sync::Arc;

#[allow(clippy::unwrap_used)] // unwrap_used is for parsing static SQL, assumed valid
#[test]
fn test_postprocess_query_statement_functions_expressions() {
    let args: [(&str, &str); 21] = [
        ("select year(ts)", "SELECT date_part('year', ts)"),
        ("select dayofyear(ts)", "SELECT date_part('doy', ts)"),
        ("select day(ts)", "SELECT date_part('day', ts)"),
        ("select dayofmonth(ts)", "SELECT date_part('day', ts)"),
        ("select dayofweek(ts)", "SELECT date_part('dow', ts)"),
        ("select month(ts)", "SELECT date_part('month', ts)"),
        ("select weekofyear(ts)", "SELECT date_part('week', ts)"),
        ("select week(ts)", "SELECT date_part('week', ts)"),
        ("select hour(ts)", "SELECT date_part('hour', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        ("select second(ts)", "SELECT date_part('second', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"), // Duplicate, but keep for consistency with original
        ("select yearofweek(ts)", "SELECT yearofweek(ts)"),
        ("select yearofweekiso(ts)", "SELECT yearofweekiso(ts)"),
        ("SELECT dateadd(year, 5, '2025-06-01')", "SELECT dateadd('year', 5, '2025-06-01')"),
        ("SELECT dateadd(\"year\", 5, '2025-06-01')", "SELECT dateadd('year', 5, '2025-06-01')"),
        ("SELECT datediff(day, 5, '2025-06-01')", "SELECT datediff('day', 5, '2025-06-01')"),
        ("SELECT datediff(week, 5, '2025-06-01')", "SELECT datediff('week', 5, '2025-06-01')"),
        ("SELECT datediff(nsecond, 10000000, '2025-06-01')", "SELECT datediff('nsecond', 10000000, '2025-06-01')"),
        ("SELECT date_diff(hour, 5, '2025-06-01')", "SELECT date_diff('hour', 5, '2025-06-01')"),
        ("SELECT date_add(us, 100000, '2025-06-01')", "SELECT date_add('us', 100000, '2025-06-01')"),
    ];

    for (init, exp) in args {
        let statement_res = DFParser::parse_sql(init);
        assert!(statement_res.is_ok(), "SQL parsing failed for: {}", init);
        let mut statement = statement_res.unwrap().pop_front();
        if let Some(mut s) = statement {
            UserQuery::postprocess_query_statement(&mut s);
            assert_eq!(s.to_string(), exp);
        } else {
            panic!("No statement found for: {}", init);
        }
    }
}
static TABLE_SETUP: &str = include_str!(r"./table_setup.sql");

// Refactored to return ExecutionResult
pub async fn create_df_session() -> ExecutionResult<Arc<UserSession>> {
    let metastore = SlateDBMetastore::new_in_memory().await; // Panics on failure, part of test setup.

    // Helper to map MetastoreError to ExecutionError
    let map_metastore_err = |e: core_metastore::error::MetastoreError, context_msg: &'static str| {
        wrap_error(e, context_msg.to_string()).context(MetastoreSnafu)
    };

    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new("test_volume".to_string(), core_metastore::VolumeType::Memory),
        )
        .await
        .map_err(|e| map_metastore_err(e, "Failed to create volume for test session"))?;
    
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
        .map_err(|e| map_metastore_err(e, "Failed to create database for test session"))?;

    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema { ident: schema_ident, properties: None },
        )
        .await
        .map_err(|e| map_metastore_err(e, "Failed to create schema for test session"))?;

    let user_session = Arc::new(
        UserSession::new(metastore) // Assuming UserSession::new now returns ExecutionResult
            .await
            .map_err(|e| { // If UserSession::new returns its own error type, map it appropriately.
                           // For now, assume it returns ExecutionError directly or an error that can be wrapped by ApplicationSnafu.
                let kind = crate::error::CoreExecutorErrorKind::InitializationFailed { service_name: "UserSession".to_string() };
                wrap_error(kind, "Failed to create user session".to_string()).context(ApplicationSnafu)
            })?
    );

    for query_str in TABLE_SETUP.split(';') {
        if !query_str.trim().is_empty() {
            let mut query = user_session.query(query_str, QueryContext::default());
            query.execute().await?; // Propagate ExecutionError if setup query fails
        }
    }
    Ok(user_session)
}

#[macro_export]
macro_rules! test_query {
    (
        $test_fn_name:ident,
        $query:expr
        $(, setup_queries =[$($setup_queries:expr),* $(,)?])?
        $(, sort_all = $sort_all:expr)?
        $(, snapshot_path = $user_snapshot_path:expr)?
    ) => {
        paste::paste! {
            #[tokio::test]
            async fn [< query_ $test_fn_name >]() {
                // create_df_session now returns a Result. Handle it.
                let ctx_res = crate::tests::query::create_df_session().await;
                if let Err(e) = &ctx_res {
                    panic!("Failed to create test session for '{}': {:?}", stringify!($test_fn_name), e);
                }
                let ctx = ctx_res.unwrap();

                // Execute all setup queries (if provided) to set up the session context
                $(
                    $(
                        {
                            let mut q = ctx.query($setup_queries, crate::query::QueryContext::default());
                            // Handle Result from setup queries
                            if let Err(e) = q.execute().await {
                                panic!("Setup query '{}' failed for test '{}': {:?}", $setup_queries, stringify!($test_fn_name), e);
                            }
                        }
                    )*
                )?

                let mut query = ctx.query($query, crate::query::QueryContext::default());
                let res = query.execute().await; // This is ExecutionResult<Vec<RecordBatch>>
                let sort_all = false $(|| $sort_all)?;

                let mut settings = insta::Settings::new();
                settings.set_description(stringify!($query));
                settings.set_omit_expression(true);
                settings.set_prepend_module_to_snapshot(false);
                settings.set_snapshot_path(concat!("snapshots", "/") $(.to_owned() + $user_snapshot_path)?);

                let setup_queries_vec: Vec<&str> = vec![$($($setup_queries),*)?]; // Corrected vec construction
                if !setup_queries_vec.is_empty() {
                    settings.set_info(&format!("Setup queries: {}", setup_queries_vec.join("; ")));
                }
                settings.bind(|| {
                    let df_snapshot_content = match res {
                        Ok(record_batches) => {
                            let mut batches = record_batches;
                            if sort_all {
                                for batch in &mut batches {
                                    *batch = df_catalog::test_utils::sort_record_batch_by_sortable_columns(batch);
                                }
                            }
                            // Format Ok result for snapshot
                            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                                .unwrap_or_else(|e| format!("Error formatting batches: {}", e)) // Handle formatting error
                                .to_string())
                        },
                        Err(e) => {
                            // Format Err result for snapshot, using the new Display impl of ExecutionError
                            Err(format!("{}", e)) // This will include context and source error string
                        }
                    };

                    // The snapshot will now contain either the formatted batches or the formatted error.
                    // For errors, it will look like: Err("{context}: {source_error_type}: {source_error_message}")
                    insta::assert_debug_snapshot!((df_snapshot_content));
                });
            }
        }
    };
}

// CREATE SCHEMA
test_query!(
    create_schema,
    "SHOW SCHEMAS IN embucket STARTS WITH 'new_schema'",
    setup_queries = ["CREATE SCHEMA embucket.new_schema"]
);

// CREATE TABLE
test_query!(
    create_table_with_timestamp_nanosecond,
    "CREATE TABLE embucket.public.ts_table (ts TIMESTAMP_NTZ(9)) as VALUES ('2025-04-09T21:11:23');"
);

// DROP TABLE
test_query!(
    drop_table,
    "SHOW TABLES IN public STARTS WITH 'test'",
    setup_queries = [
        "CREATE TABLE embucket.public.test (id INT) as VALUES (1), (2)",
        "DROP TABLE embucket.public.test"
    ]
);

// context name injection
test_query!(
    context_name_injection,
    "SHOW TABLES IN new_schema",
    setup_queries = [
        "CREATE SCHEMA embucket.new_schema",
        "SET schema = 'new_schema'",
        "CREATE table new_table (id INT)",
    ]
);

// SELECT
test_query!(select_date_add_diff, "SELECT dateadd(day, 5, '2025-06-01')");
test_query!(func_date_add, "SELECT date_add(day, 30, '2025-01-06')");
test_query!(select_star, "SELECT * FROM employee_table");
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

// SHOW DATABASES
test_query!(show_databases, "SHOW DATABASES", sort_all = true, snapshot_path = "session");
// ... (other SHOW tests remain the same, macro change handles error propagation for snapshots) ...
// SHOW SCHEMAS
test_query!(show_schemas, "SHOW SCHEMAS", sort_all = true, snapshot_path = "session");
test_query!(show_schemas_starts_with, "SHOW SCHEMAS STARTS WITH 'publ'", sort_all = true, snapshot_path = "session");
test_query!(show_schemas_in_db, "SHOW SCHEMAS IN embucket", sort_all = true, snapshot_path = "session");
test_query!(show_schemas_in_db_and_prefix, "SHOW SCHEMAS IN embucket STARTS WITH 'pub'", sort_all = true, snapshot_path = "session");

// SHOW TABLES
test_query!(show_tables, "SHOW TABLES", sort_all = true, snapshot_path = "session");
test_query!(show_tables_starts_with, "SHOW TABLES STARTS WITH 'dep'", sort_all = true, snapshot_path = "session");
test_query!(show_tables_in_schema, "SHOW TABLES IN public", sort_all = true, snapshot_path = "session");
test_query!(show_tables_in_schema_full, "SHOW TABLES IN embucket.public", sort_all = true, snapshot_path = "session");
test_query!(show_tables_in_schema_and_prefix, "SHOW TABLES IN public STARTS WITH 'dep'", sort_all = true, snapshot_path = "session");

// SHOW VIEWS
test_query!(show_views, "SHOW VIEWS", sort_all = true, snapshot_path = "session");
test_query!(show_views_starts_with, "SHOW VIEWS STARTS WITH 'schem'", sort_all = true, snapshot_path = "session");
test_query!(show_views_in_schema, "SHOW VIEWS IN information_schema", sort_all = true, snapshot_path = "session");
test_query!(show_views_in_schema_full, "SHOW VIEWS IN embucket.information_schema", sort_all = true, snapshot_path = "session");
test_query!(show_views_in_schema_and_prefix, "SHOW VIEWS IN information_schema STARTS WITH 'schem'", sort_all = true, snapshot_path = "session");

// SHOW COLUMNS
test_query!(show_columns, "SHOW COLUMNS", sort_all = true, snapshot_path = "session");
test_query!(show_columns_in_table, "SHOW COLUMNS IN employee_table", sort_all = true, snapshot_path = "session");
test_query!(show_columns_in_table_full, "SHOW COLUMNS IN embucket.public.employee_table", sort_all = true, snapshot_path = "session");
test_query!(show_columns_starts_with, "SHOW COLUMNS IN employee_table STARTS WITH 'last_'", sort_all = true, snapshot_path = "session");

// SHOW OBJECTS
test_query!(show_objects, "SHOW OBJECTS", sort_all = true, snapshot_path = "session");
test_query!(show_objects_starts_with, "SHOW OBJECTS STARTS WITH 'dep'", sort_all = true, snapshot_path = "session");
test_query!(show_objects_in_schema, "SHOW OBJECTS IN public", sort_all = true, snapshot_path = "session");
test_query!(show_objects_in_schema_full, "SHOW OBJECTS IN embucket.public", sort_all = true, snapshot_path = "session");
test_query!(show_objects_in_schema_and_prefix, "SHOW OBJECTS IN public STARTS WITH 'dep'", sort_all = true, snapshot_path = "session");

// SESSION RELATED
test_query!(alter_session_set, "SHOW VARIABLES", setup_queries = ["ALTER SESSION SET v1 = 'test'"], snapshot_path = "session");
test_query!(alter_session_unset, "SHOW VARIABLES", setup_queries = ["ALTER SESSION SET v1 = 'test' v2 = 1", "ALTER SESSION UNSET v1"], snapshot_path = "session");
test_query!(show_parameters, "SHOW PARAMETERS", sort_all = true, snapshot_path = "session");
test_query!(use_role, "SHOW VARIABLES", setup_queries = ["USE ROLE test_role"], snapshot_path = "session");
test_query!(use_secondary_roles, "SHOW VARIABLES", setup_queries = ["USE SECONDARY ROLES test_role"], snapshot_path = "session");
test_query!(use_warehouse, "SHOW VARIABLES", setup_queries = ["USE WAREHOUSE test_warehouse"], snapshot_path = "session");
test_query!(use_database, "SHOW VARIABLES", setup_queries = ["USE DATABASE test_db"], snapshot_path = "session");
test_query!(use_schema, "SHOW VARIABLES", setup_queries = ["USE SCHEMA test_schema"], snapshot_path = "session");
test_query!(show_variables_multiple, "SHOW VARIABLES", setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"], sort_all = true, snapshot_path = "session");
test_query!(set_variable, "SHOW VARIABLES", setup_queries = ["SET v1 = 'test'"], snapshot_path = "session");
test_query!(set_variable_system, "SELECT name, value FROM snowplow.information_schema.df_settings WHERE name = 'datafusion.execution.time_zone'", setup_queries = ["SET datafusion.execution.time_zone = 'TEST_TIMEZONE'"], snapshot_path = "session");
test_query!(unset_variable, "UNSET v3", setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"], snapshot_path = "session");

// EXPLAIN
test_query!(explain_select, "EXPLAIN SELECT * FROM embucket.public.employee_table", setup_queries = ["SET datafusion.explain.logical_plan_only = true"], snapshot_path = "session");
test_query!(explain_select_limit, "EXPLAIN SELECT * FROM embucket.public.employee_table limit 1", setup_queries = ["SET datafusion.explain.logical_plan_only = true"], snapshot_path = "session");
test_query!(explain_select_column, "EXPLAIN SELECT last_name FROM embucket.public.employee_table limit 1", setup_queries = ["SET datafusion.explain.logical_plan_only = true"], snapshot_path = "session");
test_query!(explain_select_missing_column, "EXPLAIN SELECT missing FROM embucket.public.employee_table limit 1", setup_queries = ["SET datafusion.explain.logical_plan_only = true"], snapshot_path = "session");

// New test for query execution error (e.g., table not found)
test_query!(
    query_non_existent_table,
    "SELECT * FROM non_existent_table_for_error_test"
);

// New test for SQL syntax error
test_query!(
    query_syntax_error,
    "SELEC * FRM some_table_for_syntax_error" // Deliberate syntax error
);

// Test for an error during setup query
// This requires manually writing the test, as the macro panics on setup error.
#[tokio::test]
async fn test_setup_query_error_path() {
    let ctx_res = crate::tests::query::create_df_session().await;
    if let Err(e) = &ctx_res {
        panic!("Failed to create test session for 'test_setup_query_error_path': {:?}", e);
    }
    let ctx = ctx_res.unwrap();
    let mut q = ctx.query("CREATE TABEL error_setup_table (id INT)", crate::query::QueryContext::default()); // Deliberate typo: TABEL
    let result = q.execute().await;
    
    assert!(result.is_err(), "Setup query with syntax error should fail.");
    let err = result.unwrap_err();
    
    // Check the error structure (this depends on how SQLErrors are wrapped into ExecutionError)
    // Assuming it becomes ExecutionError::DataFusionQuery or similar
    match err {
        ExecutionError::DataFusionQuery { source: emb_err, query: _ } => { // Or other appropriate variant like DataFusionProcessing if SQLError is wrapped
            assert!(emb_err.source.to_string().contains("Syntax error") || emb_err.source.to_string().contains("ParserError")); // DataFusionError often contains ParserError
            assert!(emb_err.context.contains("Executing SQL query")); // Context from UserSession::query
        }
        ExecutionError::Application {source: emb_err } => { // If it's mapped to CoreExecutorErrorKind -> Application
             match emb_err.source {
                crate::error::CoreExecutorErrorKind::DataFusionExecution(df_err_str) => {
                     assert!(df_err_str.contains("Syntax error") || df_err_str.contains("ParserError"));
                 }
                 _=> panic!("Incorrect CoreExecutorErrorKind for setup query error: {:?}", emb_err.source)
             }
        }
        _ => panic!("Incorrect ExecutionError variant for setup query error: {:?}", err),
    }
}
