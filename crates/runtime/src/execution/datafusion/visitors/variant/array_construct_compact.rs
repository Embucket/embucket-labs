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
    Expr, Function, FunctionArg, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, Statement, VisitorMut,
};

#[derive(Debug)]
pub struct ArrayConstructCompactRewriter;

impl VisitorMut for ArrayConstructCompactRewriter {
    type Break = bool;

    fn post_visit_expr(
        &mut self,
        expr: &mut datafusion_expr::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion_expr::sqlparser::ast::Expr::Function(Function { name, args, .. }) = expr {
            if let Some(part) = name.0.last() {
                if part.as_ident().map(|i| i.value == "array_construct_compact").unwrap_or(false) {
                    // Create the inner array_construct function
                    let array_construct = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("array_construct".to_string()))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: args.clone(),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Default::default(),
                    };

                    // Create the outer array_compact function that wraps array_construct
                    let array_compact = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("array_compact".to_string()))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![FunctionArg::Unnamed(
                                datafusion_expr::sqlparser::ast::FunctionArgExpr::Expr(Expr::Function(
                                    array_construct,
                                )),
                            )],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Default::default(),
                    };

                    *expr = Expr::Function(array_compact);
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    stmt.visit(&mut ArrayConstructCompactRewriter {});
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::udfs::variant::{array_construct, array_compact};
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_construct_compact_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct and array_compact UDFs
        ctx.state().register_udf(array_construct::get_udf());
        ctx.state().register_udf(array_compact::get_udf());

        // Test array_construct_compact rewrite
        let sql = "SELECT array_construct_compact(1, NULL, 2, NULL, 3) as compact_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| compact_arr |",
                "+-------------+",
                "| [1,2,3]     |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
