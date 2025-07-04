use datafusion::logical_expr::sqlparser::ast::{DataType, ObjectNamePart};
use datafusion::sql::sqlparser::ast::Expr;
use datafusion_expr::sqlparser::ast::JsonPathElem;
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, Value as ASTValue, VisitorMut,
};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct TimestampVisitor {}

impl VisitorMut for TimestampVisitor {
    type Break = ();

    /*
        Cast {
        kind: DoubleColon,
        expr: Value(
            ValueWithSpan {
                value: Number(
                    "1000000000",
                    false,
                ),
                span: Span(Location(1,8)..Location(1,18)),
            },
        ),
        data_type: Custom(
            ObjectName(
                [
                    Identifier(
                        Ident {
                            value: "TIMESTAMP_TZ",
                            quote_style: None,
                            span: Span(Location(1,20)..Location(1,32)),
                        },
                    ),
                ],
            ),
            [],
        ),
        format: None,
    }
         */

    /*
        Cast {
        kind: DoubleColon,
        expr: Value(
            ValueWithSpan {
                value: Number(
                    "1000000000",
                    false,
                ),
                span: Span(Location(1,8)..Location(1,18)),
            },
        ),
        data_type: Timestamp(
            None,
            None,
        ),
        format: None,
    }
         */
    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> ControlFlow<Self::Break> {
        *expr = match expr.clone() {
            ASTExpr::Cast {
                expr: cast_expr,
                data_type,
                ..
            } => match data_type {
                DataType::Timestamp(_, _) => ASTExpr::Function(Function {
                    name: ObjectName::from(vec![Ident::new("to_timestamp")]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(*cast_expr))],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                }),
                DataType::Custom(ObjectName(v), ..) => match &v[0] {
                    ObjectNamePart::Identifier(ident) => {
                        let name = ident.value.to_ascii_lowercase();
                        match name.as_str() {
                            "timestamp_tz" | "timestamp_ntz" | "timestamp_ltz" => {
                                ASTExpr::Function(Function {
                                    name: ObjectName::from(vec![Ident::new(format!("to_{name}"))]),
                                    uses_odbc_syntax: false,
                                    parameters: FunctionArguments::None,
                                    args: FunctionArguments::List(FunctionArgumentList {
                                        duplicate_treatment: None,
                                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            *cast_expr,
                                        ))],
                                        clauses: vec![],
                                    }),
                                    filter: None,
                                    null_treatment: None,
                                    over: None,
                                    within_group: vec![],
                                })
                            }
                            other => expr.clone(),
                        }
                    }
                },
                _ => expr.clone(),
            },
            other => other,
        };

        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut TimestampVisitor {});
}
