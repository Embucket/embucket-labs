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
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, Value, ValueWithSpan, VisitorMut,
};
use datafusion_expr::sqlparser::tokenizer::Span;
use datafusion_expr::sqlparser::tokenizer::Location;

#[derive(Debug, Default)]
pub struct VariantElementVisitor {}

impl VariantElementVisitor {
    pub fn new() -> Self {
        Self {}
    }
}

impl VisitorMut for VariantElementVisitor {
    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> std::ops::ControlFlow<Self::Break> {
        if let ASTExpr::JsonAccess { value, path } = expr {
            let mut path = path.to_string();
            if path.starts_with(":") {
                path = format!(".{}", path.split_at(1).1);
            }
            let path = format!("${path}");
            let new_expr = ASTExpr::Function(Function {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("variant_element".to_string()))]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*value.clone())),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ASTExpr::Value(
                            ValueWithSpan { value: Value::SingleQuotedString(path.to_string()), span: Span::new(Location::new(0,0), Location::new(0,0)) },
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ASTExpr::Value(
                            ValueWithSpan { value: Value::Boolean(true), span: Span::new(Location::new(0,0), Location::new(0,0)) },
                        ))),
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
            *expr = new_expr;
        }
        std::ops::ControlFlow::Continue(())
    }
    type Break = bool;
}

pub fn visit(stmt: &mut Statement) {
    stmt.visit(&mut VariantElementVisitor::new());
}

// For unit tests see udfs/variant_element.rs
