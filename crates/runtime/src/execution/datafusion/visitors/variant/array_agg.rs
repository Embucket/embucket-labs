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

use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgumentList, FunctionArguments, Ident, JsonPath,
    JsonPathElem, ObjectName, ObjectNamePart, Statement, VisitorMut, Value, ValueWithSpan, 
};
use sqlparser::tokenizer::{Location, Span};

#[derive(Debug)]
pub struct VariantArrayAggRewriter;

impl VisitorMut for VariantArrayAggRewriter {
    type Break = bool;

    fn post_visit_expr(
        &mut self,
        expr: &mut datafusion_expr::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion_expr::sqlparser::ast::Expr::Function(Function { name, .. }) = expr {
            if let Some(part) = name.0.last() {
                if part.as_ident().map(|i| i.value == "array_agg").unwrap_or(false) {
                    let wrapped_function = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("array_construct".to_string()))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![FunctionArg::Unnamed(
                                datafusion_expr::sqlparser::ast::FunctionArgExpr::Expr(expr.clone()),
                            )],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Default::default(),
                    };
                    let fn_call_expr =
                        datafusion_expr::sqlparser::ast::Expr::Function(wrapped_function);

                    let index_expr = Expr::JsonAccess {
                        value: Box::new(fn_call_expr),
                        path: JsonPath {
                            path: vec![JsonPathElem::Bracket {
                                key: Expr::Value(ValueWithSpan {
                                    value: Value::Number("0".to_string(), false),
                                    span: Span::new(Location::new(0,0), Location::new(0,0)),
                                }),
                            }],
                        },
                    };

                    *expr = index_expr;
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    stmt.visit(&mut VariantArrayAggRewriter {});
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::udfs::variant::{array_construct, variant_element};
    use crate::execution::datafusion::visitors::variant::variant_element as variant_element_visitor;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_agg_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct UDF

        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(variant_element::get_udf());

        // Create table and insert data
        let create_sql = "CREATE TABLE test_table (id INT, val INT)";
        let mut create_stmt = ctx.state().sql_to_statement(create_sql, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = create_stmt {
            visit(stmt);
        }
        ctx.sql(&create_stmt.to_string()).await?.collect().await?;

        let insert_sql = "INSERT INTO test_table VALUES (1, 1), (1, 2), (1, 3)";
        ctx.sql(insert_sql).await?.collect().await?;

        // Test array_agg rewrite by validating JSON output
        let sql = "SELECT array_agg(val) as json_arr FROM test_table GROUP BY id";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
            variant_element_visitor::visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| json_arr |",
                "+----------+",
                "| [1,2,3]  |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }
}
