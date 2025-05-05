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
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, VisitorMut,
};

#[derive(Debug, Default)]
pub struct ArrayConstructVisitor;

impl ArrayConstructVisitor {
    pub fn new() -> Self {
        Self
    }
}

impl VisitorMut for ArrayConstructVisitor {
    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> std::ops::ControlFlow<Self::Break> {
        if let ASTExpr::Array(elements) = expr {
            let args = elements
                .elem
                .iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
                .collect();

            let new_expr = ASTExpr::Function(Function {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("array_construct"))]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args,
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });
            *expr = new_expr;
        }
        std::ops::ControlFlow::Continue(())
    }
    type Break = bool;
}

pub fn visit(stmt: &mut Statement) {
    stmt.visit(&mut ArrayConstructVisitor::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::udfs::variant::array_construct;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion::execution::FunctionRegistry;

    #[tokio::test]
    async fn test_array_construct_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct UDF
        ctx.state().register_udf(array_construct::get_udf());

        // Test simple array construction
        let sql = "SELECT [1, 2, 3] as arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| arr     |",
                "+---------+",
                "| [1,2,3] |",
                "+---------+",
            ],
            &result
        );

        // Test array with mixed types
        let sql = "SELECT [1, 'test', null] as mixed_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------------+",
                "| mixed_arr       |",
                "+-----------------+",
                "| [1,\"test\",null] |",
                "+-----------------+",
            ],
            &result
        );

        // Test nested arrays
        let sql = "SELECT [[1, 2], [3, 4]] as nested_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }

        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| nested_arr    |",
                "+---------------+",
                "| [[1,2],[3,4]] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
