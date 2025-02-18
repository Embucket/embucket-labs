use std::ops::ControlFlow;
use datafusion::logical_expr::sqlparser::ast::{VisitorMut, Expr};
use datafusion_expr::ExprFunctionExt;
use sqlparser::ast::Function;

struct StatementPostProcessor;

impl StatementPostProcessor {
    fn yes<T>(value: Expr) {
        matches!(value, T);
    }
}

impl VisitorMut for StatementPostProcessor {
    type Break = ();
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(Function { name, args, .. }) = expr {
            
        }
        ControlFlow::Continue(())
    }
}