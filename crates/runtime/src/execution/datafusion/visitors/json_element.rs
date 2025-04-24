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
    FunctionArguments, Ident, ObjectName, Statement, Value as ASTValue, VisitorMut,
};
use sqlparser::ast::JsonPathElem;
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct JsonVisitor {}

impl VisitorMut for JsonVisitor {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> ControlFlow<Self::Break> {
        if let ASTExpr::JsonAccess { .. } = expr {
            *expr = convert_json_access(expr.clone());
        }
        ControlFlow::Continue(())
    }
}

fn convert_json_access(expr: ASTExpr) -> ASTExpr {
    match expr {
        ASTExpr::JsonAccess { value, path } => {
            let mut base = convert_json_access(*value);

            for elem in path.path {
                let key_expr = match elem {
                    JsonPathElem::Dot { key, .. } => {
                        ASTExpr::Value(ASTValue::SingleQuotedString(key))
                    }
                    JsonPathElem::Bracket { key } => key,
                };

                base = ASTExpr::Function(Function {
                    name: ObjectName(vec![Ident::new("json_get")]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(base)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(key_expr)),
                        ],
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
            }

            base
        }
        other => other,
    }
}

pub fn visit(stmt: &mut Statement) {
    stmt.visit(&mut JsonVisitor {});
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;

    #[tokio::test]
    async fn test_json_access_rewrite() -> DFResult<()> {
        let state = SessionContext::new().state();

        let cases = vec![
            (
                "SELECT context[0]::varchar",
                "SELECT json_get(context, 0)::VARCHAR",
            ),
            (
                "SELECT context[0]:id::varchar",
                "SELECT json_get(json_get(context, 0), 'id')::VARCHAR",
            ),
            (
                "SELECT context[0].id::varchar",
                "SELECT json_get(json_get(context, 0), 'id')::VARCHAR",
            ),
            (
                "SELECT context[0]:id[1]::varchar",
                "SELECT json_get(json_get(json_get(context, 0), 'id'), 1)::VARCHAR",
            ),
        ];

        for (input, expected) in cases {
            let mut statement = state.sql_to_statement(input, "snowflake")?;

            if let DFStatement::Statement(ref mut stmt) = statement {
                visit(stmt);
            }

            assert_eq!(statement.to_string(), expected);
        }
        Ok(())
    }
}
