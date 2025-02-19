use std::ops::ControlFlow;
use datafusion::logical_expr::sqlparser::ast::{VisitorMut, Expr};
use datafusion_expr::ExprFunctionExt;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName};

struct StatementPostProcessor;

impl VisitorMut for StatementPostProcessor {
    type Break = ();
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(Function { name: ObjectName(idents), args: FunctionArguments::List(FunctionArgumentList { args, .. }), .. }) = expr  {
            match idents.first().unwrap().value.as_str() {
                "dateadd" | "date_add" | "datediff" | "date_diff" => {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident { value, .. }))) = args.iter_mut().next().unwrap() {
                        *value = format!("'{value}'");
                    }
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}