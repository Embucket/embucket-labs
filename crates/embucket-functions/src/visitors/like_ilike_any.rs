use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    Statement, VisitorMut,
};
use datafusion_expr::sqlparser::ast::{Function, VisitMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct LikeILikeAny;

impl VisitorMut for LikeILikeAny {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Like { negated, any, expr: inner_expr, pattern, escape_char, } = expr {
            if *any {
                tracing::error!("Pattern: {:?}", *pattern);
            }
            tracing::error!("AST expr: {:?}", expr);
        } else if let Expr::ILike { negated, any, expr: inner_expr, pattern, escape_char, } = expr {
            if *any {
                tracing::error!("Pattern:  {:?}", *pattern);
            }
            tracing::error!("AST expr: {:?}", expr);
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut LikeILikeAny {});
}