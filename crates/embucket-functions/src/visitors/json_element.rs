use datafusion_expr::sqlparser::ast::JsonPathElem;
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, Value as ASTValue, VisitorMut,
};
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
                        ASTExpr::Value(ASTValue::SingleQuotedString(key).into())
                    }
                    JsonPathElem::Bracket { key } => key,
                };

                base = ASTExpr::Function(Function {
                    name: ObjectName::from(vec![Ident::new("json_get")]),
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
    let _ = stmt.visit(&mut JsonVisitor {});
}
