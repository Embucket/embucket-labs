use crate::execution::query::{QueryContext, UserQuery};
use crate::execution::session::UserSession;

use crate::execution::error::{ExecutionError, ExecutionResult};
use crate::execution::service::ExecutionService;
use crate::execution::utils::{Config, DataSerializationFormat};
use crate::SlateDBMetastore;
use datafusion::assert_batches_eq;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::visit_expressions;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName};
use embucket_metastore::Metastore;
use embucket_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    TableIdent as MetastoreTableIdent, Volume as MetastoreVolume,
};
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
                    name: ObjectName(idents),
                    args: FunctionArguments::List(FunctionArgumentList { args, .. }),
                    ..
                }) = expr
                {
                    match idents.first().unwrap().value.as_str() {
                        "dateadd" | "date_add" | "datediff" | "date_diff" => {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(ident)) =
                                args.iter().next().unwrap()
                            {
                                if let Expr::Value(found) = ident {
                                    if test.should_work {
                                        assert_eq!(*found, test.expected);
                                    } else {
                                        assert_ne!(*found, test.expected);
                                    }
                                }
                            }
                        }
                        _ => {}
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
        QueryContext {
            database: Some("db2".to_string()),
            schema: Some("sch2".to_string()),
        },
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
            vec![("catalog".to_string(), "db3".to_string())]
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
                ("catalog".to_string(), "db4".to_string()),
                ("schema".to_string(), "sch4".to_string()),
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
    let query = format!("CREATE TABLE {}.{}.{} (id INT, ts TIMESTAMP_NTZ(9)) as VALUES (1, '2025-04-09T21:11:23'), (2, '2025-04-09T21:11:00');", table_ident.database, table_ident.schema, table_ident.table);
    let (rows, _) = execution_svc
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
    let (rows, _) = execution_svc
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
async fn test_resolve_table_ident() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = UserSession::new(metastore)
        .await
        .expect("Failed to create session");
    let query = UserQuery::new(Arc::from(session), "", QueryContext::default());

    let test_cases: [(Vec<Ident>, ExecutionResult<String>); 4] = [
        (
            vec![Ident::new("table")],
            Ok("embucket.public.table".to_string()),
        ),
        (
            vec![Ident::new("test_schema"), Ident::new("table")],
            Ok("embucket.test_schema.table".to_string()),
        ),
        (
            vec![
                Ident::new("test_db"),
                Ident::new("test_schema"),
                Ident::new("table"),
            ],
            Ok("test_db.test_schema.table".to_string()),
        ),
        (
            vec![
                Ident::new("test_db"),
                Ident::new("test_schema"),
                Ident::new("table"),
                Ident::new("col"),
            ],
            Err(ExecutionError::InvalidTableIdentifier {
                ident: "test_db.test_schema.table.col".to_string(),
            }),
        ),
    ];
    for (test_case, expected) in test_cases {
        let result = query.resolve_table_ident(test_case.clone());
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
async fn test_resolve_schema_ident() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session = UserSession::new(metastore)
        .await
        .expect("Failed to create session");
    let query = UserQuery::new(Arc::from(session), "", QueryContext::default());

    let test_cases: [(Vec<Ident>, ExecutionResult<String>); 3] = [
        (
            vec![Ident::new("schema")],
            Ok("embucket.schema".to_string()),
        ),
        (
            vec![Ident::new("test_db"), Ident::new("schema")],
            Ok("test_db.schema".to_string()),
        ),
        (
            vec![
                Ident::new("test_db"),
                Ident::new("test_schema"),
                Ident::new("table"),
            ],
            Err(ExecutionError::InvalidSchemaIdentifier {
                ident: "test_db.test_schema.table".to_string(),
            }),
        ),
    ];
    for (test_case, expected) in test_cases {
        let res = query.resolve_schema_ident(test_case.clone());
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

async fn prepare_env() -> (ExecutionService, Arc<SlateDBMetastore>, String) {
    let metastore = SlateDBMetastore::new_in_memory().await;
    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                embucket_metastore::VolumeType::Memory,
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

    let execution_svc = ExecutionService::new(
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
