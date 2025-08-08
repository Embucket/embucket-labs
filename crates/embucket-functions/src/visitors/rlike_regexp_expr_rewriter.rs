use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    Statement, VisitorMut,
};
use datafusion_expr::sqlparser::ast::{Function, VisitMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct RLikeRegexpExprRewriter;

impl VisitorMut for RLikeRegexpExprRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::RLike {
            expr: inner_expr,
            negated,
            pattern,
            ..
        } = expr
        {
            let function = Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("regexp_like")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*inner_expr.clone())),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*pattern.clone())),
                    ],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });
            if *negated {
                *expr = Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(function),
                };
            } else {
                *expr = function;
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut RLikeRegexpExprRewriter {});
}
