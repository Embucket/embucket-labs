use crate::models::QueryResultData;
use crate::query::{QueryContext, UserQuery};
use crate::session::{SessionProperty, UserSession};

use crate::error::{ExecutionError, ExecutionResult};
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::{Config, DataSerializationFormat};
use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    TableIdent as MetastoreTableIdent, Volume as MetastoreVolume,
};
use datafusion::assert_batches_eq;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::sqlparser::ast::visit_expressions;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, ObjectNamePart};
use df_catalog::test_utils::sort_record_batch_by_sortable_columns;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
};
use sqlparser::ast::{SetExpr, Value};
use std::ops::ControlFlow;
use std::sync::Arc;

struct Test<'a, T> {
    input: &'a str,
    expected: T,
    should_work: bool,
}
impl<'a, T> Test<'a, T> {
    pub const fn new(input: &'a str, expected: T, should_work: bool) -> Self {
        Self {
            input,
            expected,
            should_work,
        }
    }
}
#[tokio::test]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::explicit_iter_loop,
    clippy::collapsible_match
)]
async fn test_timestamp_keywords_postprocess() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );
    let query_context = QueryContext::default();
    let test = vec![
        Test::new(
            "SELECT dateadd(year, 5, '2025-06-01')",
            Value::SingleQuotedString("year".to_owned()),
            true,
        ),
        Test::new(
            "SELECT dateadd(\"year\", 5, '2025-06-01')",
            Value::SingleQuotedString("year".to_owned()),
            true,
        ),
        Test::new(
            "SELECT dateadd('year', 5, '2025-06-01')",
            Value::SingleQuotedString("year".to_owned()),
            true,
        ),
        Test::new(
            "SELECT dateadd(\"'year'\", 5, '2025-06-01')",
            Value::SingleQuotedString("year".to_owned()),
            false,
        ),
        Test::new(
            "SELECT dateadd(\'year\', 5, '2025-06-01')",
            Value::SingleQuotedString("year".to_owned()),
            true,
        ),
        Test::new(
            "SELECT datediff(day, 5, '2025-06-01')",
            Value::SingleQuotedString("day".to_owned()),
            true,
        ),
        Test::new(
            "SELECT datediff('week', 5, '2025-06-01')",
            Value::SingleQuotedString("week".to_owned()),
            true,
        ),
        Test::new(
            "SELECT datediff(nsecond, 10000000, '2025-06-01')",
            Value::SingleQuotedString("nsecond".to_owned()),
            true,
        ),
        Test::new(
            "SELECT date_diff(hour, 5, '2025-06-01')",
            Value::SingleQuotedString("hour".to_owned()),
            true,
        ),
        Test::new(
            "SELECT date_add(us, 100000, '2025-06-01')",
            Value::SingleQuotedString("us".to_owned()),
            true,
        ),
    ];
    for test in test.iter() {
        let query = session.query(test.input, query_context.clone());
        let mut statement = query.parse_query().unwrap();
        UserQuery::postprocess_query_statement(&mut statement);
        if let DFStatement::Statement(statement) = statement {
            visit_expressions(&statement, |expr| {
                if let Expr::Function(Function {
                    name: ObjectName(object_name_parts),
                    args: FunctionArguments::List(FunctionArgumentList { args, .. }),
                    ..
                }) = expr
                {
                    match object_name_parts.first().unwrap() {
                        ObjectNamePart::Identifier(ident) => match ident.value.as_str() {
                            "dateadd" | "date_add" | "datediff" | "date_diff" => {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(ident)) =
                                    args.iter().next().unwrap()
                                {
                                    if let Expr::Value(found) = ident {
                                        if test.should_work {
                                            assert_eq!(
                                                found.to_string(),
                                                test.expected.to_string()
                                            );
                                        } else {
                                            assert_ne!(
                                                found.to_string(),
                                                test.expected.to_string()
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {}
                        },
                    }
                }
                ControlFlow::<()>::Continue(())
            });
        }
    }
}

#[allow(clippy::unwrap_used)]
#[test]
fn test_postprocess_query_statement_functions_expressions() {
    let args: [(&str, &str); 14] = [
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
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        // Do nothing
        ("select yearofweek(ts)", "SELECT yearofweek(ts)"),
        ("select yearofweekiso(ts)", "SELECT yearofweekiso(ts)"),
    ];

    for (init, exp) in args {
        let statement = DFParser::parse_sql(init).unwrap().pop_front();
        if let Some(mut s) = statement {
            UserQuery::postprocess_query_statement(&mut s);
            assert_eq!(s.to_string(), exp);
        }
    }
}

#[tokio::test]
#[allow(clippy::expect_used, clippy::manual_let_else, clippy::too_many_lines)]
async fn test_context_name_injection() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );
    let query1 = session.query("SELECT * FROM table1", QueryContext::default());
    let query_statement = if let DFStatement::Statement(statement) =
        query1.parse_query().expect("Failed to parse query")
    {
        if let SQLStatement::Query(query) = *statement {
            query
        } else {
            panic!("Failed to parse query");
        }
    } else {
        panic!("Failed to parse query");
    };
    let select_statement1 = if let SetExpr::Select(select) = *query_statement.body {
        select
    } else {
        panic!("Failed to parse query");
    };
    let from1 = select_statement1.from;

    let query2 = session.query(
        "SELECT * from table2",
        QueryContext::new(Some("db2".to_string()), Some("sch2".to_string()), None),
    );
    let query_statement2 = if let DFStatement::Statement(statement) =
        query2.parse_query().expect("Failed to parse query")
    {
        if let SQLStatement::Query(query) = *statement {
            query
        } else {
            panic!("Failed to parse query");
        }
    } else {
        panic!("Failed to parse query");
    };
    let select_statement2 = if let SetExpr::Select(select) = *query_statement2.body {
        select
    } else {
        panic!("Failed to parse query");
    };
    let from2 = select_statement2.from;

    // This will also test the default public schema
    session
        .set_session_variable(
            true,
            vec![(
                "catalog".to_string(),
                SessionProperty::from_str("db3".to_string()),
            )]
            .into_iter()
            .collect(),
        )
        .expect("Failed to set session variable");
    let query3 = session.query("SELECT * from table3", QueryContext::default());
    let query_statement3 = if let DFStatement::Statement(statement) =
        query3.parse_query().expect("Failed to parse query")
    {
        if let SQLStatement::Query(query) = *statement {
            query
        } else {
            panic!("Failed to parse query");
        }
    } else {
        panic!("Failed to parse query");
    };
    let select_statement3 = if let SetExpr::Select(select) = *query_statement3.body {
        select
    } else {
        panic!("Failed to parse query");
    };
    let from3 = select_statement3.from;

    // Test
    session
        .set_session_variable(
            true,
            vec![
                (
                    "catalog".to_string(),
                    SessionProperty::from_str("db4".to_string()),
                ),
                (
                    "schema".to_string(),
                    SessionProperty::from_str("sch4".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        )
        .expect("Failed to set session variable");
    let query4 = session.query(
        "SELECT * from table4 INNER JOIN table4_1 ON 1=1",
        QueryContext::default(),
    );
    let query_statement4 = if let DFStatement::Statement(statement) =
        query4.parse_query().expect("Failed to parse query")
    {
        if let SQLStatement::Query(query) = *statement {
            query
        } else {
            panic!("Failed to parse query");
        }
    } else {
        panic!("Failed to parse query");
    };
    let select_statement4 = if let SetExpr::Select(select) = *query_statement4.body {
        select
    } else {
        panic!("Failed to parse query");
    };
    let from4 = select_statement4.from;
    insta::assert_debug_snapshot!((from1, from2, from3, from4));
}

#[tokio::test]
async fn test_create_table_with_timestamp_nanosecond() {
    let (execution_svc, _, session_id) = prepare_env().await;
    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };
    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!(
        "CREATE TABLE {}.{}.{} (id INT, ts TIMESTAMP_NTZ(9)) as VALUES (1, '2025-04-09T21:11:23'), (2, '2025-04-09T21:11:00');",
        table_ident.database, table_ident.schema, table_ident.table
    );
    let QueryResultData { records: rows, .. } = execution_svc
        .query(&session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &rows
    );
}

#[tokio::test]
async fn test_drop_table() {
    let (execution_svc, _, session_id) = prepare_env().await;
    let table_ident = MetastoreTableIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
        table: "target_table".to_string(),
    };
    // Verify that the file was uploaded successfully by running select * from the table
    let query = format!("CREATE TABLE {table_ident} (id INT) as VALUES (1), (2);");
    let QueryResultData { records: rows, .. } = execution_svc
        .query(&session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &rows
    );

    let query = format!("DROP TABLE {table_ident};");
    execution_svc
        .query(&session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");

    // Verify that the table is not exists
    let query = format!("SELECT * FROM {table_ident};");
    let res = execution_svc
        .query(&session_id, &query, QueryContext::default())
        .await;

    assert!(res.is_err());
    if let Err(err) = res {
        assert_eq!(
            err.to_string(),
            format!("DataFusion error: Error during planning: table '{table_ident}' not found")
        );
    }
}

#[tokio::test]
async fn test_create_schema() {
    let (execution_svc, metastore, session_id) = prepare_env().await;
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public_new".to_string(),
    };
    let query = format!("CREATE SCHEMA {schema_ident};");
    execution_svc
        .query(&session_id, &query, QueryContext::default())
        .await
        .expect("Failed to execute query");
    // TODO use "SHOW SCHEMAS" sql
    metastore
        .get_schema(&schema_ident)
        .await
        .expect("Failed to get schema");
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn test_resolve_table_object_name() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = UserSession::new(metastore)
        .await
        .expect("Failed to create session");
    let query = UserQuery::new(Arc::from(session), "", QueryContext::default());

    let test_cases: [(Vec<ObjectNamePart>, ExecutionResult<String>); 4] = [
        (
            vec![ObjectNamePart::Identifier(Ident::new("table"))],
            Ok("embucket.public.table".to_string()),
        ),
        (
            vec![
                ObjectNamePart::Identifier(Ident::new("test_schema")),
                ObjectNamePart::Identifier(Ident::new("table")),
            ],
            Ok("embucket.test_schema.table".to_string()),
        ),
        (
            vec![
                ObjectNamePart::Identifier(Ident::new("test_db")),
                ObjectNamePart::Identifier(Ident::new("test_schema")),
                ObjectNamePart::Identifier(Ident::new("table")),
            ],
            Ok("test_db.test_schema.table".to_string()),
        ),
        (
            vec![
                ObjectNamePart::Identifier(Ident::new("test_db")),
                ObjectNamePart::Identifier(Ident::new("test_schema")),
                ObjectNamePart::Identifier(Ident::new("table")),
                ObjectNamePart::Identifier(Ident::new("col")),
            ],
            Err(ExecutionError::InvalidTableIdentifier {
                ident: "test_db.test_schema.table.col".to_string(),
            }),
        ),
    ];
    for (test_case, expected) in test_cases {
        let result = query.resolve_table_object_name(test_case.clone());
        if result.is_err() {
            assert_eq!(
                result.err().unwrap().to_string(),
                expected.err().unwrap().to_string()
            );
        } else {
            assert_eq!(result.unwrap().to_string(), expected.unwrap());
        }
    }
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn test_resolve_schema_object_name() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = UserSession::new(metastore)
        .await
        .expect("Failed to create session");
    let query = UserQuery::new(Arc::from(session), "", QueryContext::default());

    let test_cases: [(Vec<ObjectNamePart>, ExecutionResult<String>); 3] = [
        (
            vec![ObjectNamePart::Identifier(Ident::new("schema"))],
            Ok("embucket.schema".to_string()),
        ),
        (
            vec![
                ObjectNamePart::Identifier(Ident::new("test_db")),
                ObjectNamePart::Identifier(Ident::new("schema")),
            ],
            Ok("test_db.schema".to_string()),
        ),
        (
            vec![
                ObjectNamePart::Identifier(Ident::new("test_db")),
                ObjectNamePart::Identifier(Ident::new("test_schema")),
                ObjectNamePart::Identifier(Ident::new("table")),
            ],
            Err(ExecutionError::InvalidSchemaIdentifier {
                ident: "test_db.test_schema.table".to_string(),
            }),
        ),
    ];
    for (test_case, expected) in test_cases {
        let res = query.resolve_schema_object_name(test_case.clone());
        if res.is_err() {
            assert_eq!(
                res.err().unwrap().to_string(),
                expected.err().unwrap().to_string()
            );
        } else {
            assert_eq!(res.unwrap().to_string(), expected.unwrap());
        }
    }
}

async fn prepare_env() -> (CoreExecutionService, Arc<SlateDBMetastore>, String) {
    let metastore = SlateDBMetastore::new_in_memory().await;
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

    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        Config {
            dbt_serialization_format: DataSerializationFormat::Json,
        },
    );

    let session_id = "test_session_id";
    execution_svc
        .create_session(session_id.to_string())
        .await
        .expect("Failed to create session");
    (execution_svc, metastore, session_id.to_string())
}

static TABLE_SETUP: &str = include_str!(r"./table_setup.sql");

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn create_df_session() -> Arc<UserSession> {
    let metastore = SlateDBMetastore::new_in_memory().await;
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

    let user_session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );

    for query in TABLE_SETUP.split(';') {
        if !query.is_empty() {
            let mut query = user_session.query(query, QueryContext::default());
            query.execute().await.unwrap();
            //ctx.sql(query).await.unwrap().collect().await.unwrap();
        }
    }
    user_session
}

#[macro_export]
macro_rules! test_query {
    (
        $test_fn_name:ident,
        $query:expr
        $(, pre_queries = [$($pre_queries:expr),* $(,)?])?
        $(, sort_all = $sort_all:expr)?
    ) => {
        paste::paste! {
            #[tokio::test]
            async fn [< query_ $test_fn_name >]() {
                let ctx = create_df_session().await;

                // Execute all pre-queries (if provided) to set up the session context
                $(
                    $(
                        {
                            let mut q = ctx.query($pre_queries, crate::query::QueryContext::default());
                            q.execute().await.unwrap();
                        }
                    )*
                )?

                let mut query = ctx.query($query, crate::query::QueryContext::default());
                let res = query.execute().await;
                let sort_all = false $(|| $sort_all)?;

                insta::with_settings!({
                    description => stringify!($query),
                    omit_expression => true,
                    prepend_module_to_snapshot => false
                }, {
                    let df = match res {
                        Ok(mut record_batches) => {
                            if sort_all {
                                for batch in &mut record_batches {
                                    *batch = sort_record_batch_by_sortable_columns(batch);
                                }
                            }
                            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string())
                        },
                        Err(e) => Err(format!("Error: {e}"))
                    };

                    let df = df.map(|df| df.split('\n').map(|s| s.to_string()).collect::<Vec<String>>());
                    insta::assert_debug_snapshot!((df));
                });
            }
        }
    };
}

test_query!(select_date_add_diff, "SELECT dateadd(day, 5, '2025-06-01')");
test_query!(func_date_add, "SELECT date_add(day, 30, '2025-01-06')");
// // SELECT
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

// SHOW DATABASES
test_query!(show_databases, "SHOW DATABASES", sort_all = true);

// SHOW SCHEMAS
test_query!(show_schemas, "SHOW SCHEMAS", sort_all = true);
test_query!(
    show_schemas_starts_with,
    "SHOW SCHEMAS STARTS WITH 'publ'",
    sort_all = true
);
test_query!(
    show_schemas_in_db,
    "SHOW SCHEMAS IN embucket",
    sort_all = true
);
test_query!(
    show_schemas_in_db_and_prefix,
    "SHOW SCHEMAS IN embucket STARTS WITH 'pub'",
    sort_all = true
);

// SHOW TABLES
test_query!(show_tables, "SHOW TABLES", sort_all = true);
test_query!(
    show_tables_starts_with,
    "SHOW TABLES STARTS WITH 'dep'",
    sort_all = true
);
test_query!(
    show_tables_in_schema,
    "SHOW TABLES IN public",
    sort_all = true
);
test_query!(
    show_tables_in_schema_full,
    "SHOW TABLES IN embucket.public",
    sort_all = true
);
test_query!(
    show_tables_in_schema_and_prefix,
    "SHOW TABLES IN public STARTS WITH 'dep'",
    sort_all = true
);

// SHOW VIEWS
test_query!(show_views, "SHOW VIEWS", sort_all = true);
test_query!(
    show_views_starts_with,
    "SHOW VIEWS STARTS WITH 'schem'",
    sort_all = true
);
test_query!(
    show_views_in_schema,
    "SHOW VIEWS IN information_schema",
    sort_all = true
);
test_query!(
    show_views_in_schema_full,
    "SHOW VIEWS IN embucket.information_schema",
    sort_all = true
);
test_query!(
    show_views_in_schema_and_prefix,
    "SHOW VIEWS IN information_schema STARTS WITH 'schem'",
    sort_all = true
);

// SHOW COLUMNS
test_query!(show_columns, "SHOW COLUMNS", sort_all = true);
test_query!(
    show_columns_in_table,
    "SHOW COLUMNS IN employee_table",
    sort_all = true
);
test_query!(
    show_columns_in_table_full,
    "SHOW COLUMNS IN embucket.public.employee_table",
    sort_all = true
);
test_query!(
    show_columns_starts_with,
    "SHOW COLUMNS IN employee_table STARTS WITH 'last_'",
    sort_all = true
);

// SHOW OBJECTS
test_query!(show_objects, "SHOW OBJECTS", sort_all = true);
test_query!(
    show_objects_starts_with,
    "SHOW OBJECTS STARTS WITH 'dep'",
    sort_all = true
);
test_query!(
    show_objects_in_schema,
    "SHOW OBJECTS IN public",
    sort_all = true
);
test_query!(
    show_objects_in_schema_full,
    "SHOW OBJECTS IN embucket.public",
    sort_all = true
);
test_query!(
    show_objects_in_schema_and_prefix,
    "SHOW OBJECTS IN public STARTS WITH 'dep'",
    sort_all = true
);

// SHOW VARIABLES
test_query!(
    show_variables_multiple,
    "SHOW VARIABLES",
    pre_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    sort_all = true
);
