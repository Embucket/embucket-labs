// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::execution::query::{IceBucketQuery, IceBucketQueryContext};
use crate::execution::session::IceBucketUserSession;

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::visit_expressions;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName};
use icebucket_metastore::SlateDBMetastore;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
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
    let session =
        Arc::new(IceBucketUserSession::new(metastore).expect("Failed to create user session"));
    let query_context = IceBucketQueryContext::default();
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
        IceBucketQuery::postprocess_query_statement(&mut statement);
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
            IceBucketQuery::postprocess_query_statement(&mut s);
            assert_eq!(s.to_string(), exp);
        }
    }
}

#[tokio::test]
#[allow(clippy::expect_used, clippy::manual_let_else)]
async fn test_context_name_injection() {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let session =
        Arc::new(IceBucketUserSession::new(metastore).expect("Failed to create user session"));
    let query1 = session.query("SELECT * FROM table1", IceBucketQueryContext::default());
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
        IceBucketQueryContext {
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
    let query3 = session.query("SELECT * from table3", IceBucketQueryContext::default());
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
        IceBucketQueryContext::default(),
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
