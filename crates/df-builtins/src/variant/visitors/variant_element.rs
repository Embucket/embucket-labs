use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, Value, ValueWithSpan,
    VisitorMut,
};
use datafusion_expr::sqlparser::tokenizer::Location;
use datafusion_expr::sqlparser::tokenizer::Span;

#[derive(Debug, Default)]
pub struct VariantElementVisitor {}

impl VariantElementVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

impl VisitorMut for VariantElementVisitor {
    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> std::ops::ControlFlow<Self::Break> {
        if let ASTExpr::JsonAccess { value, path } = expr {
            let mut path = path.to_string();
            if path.starts_with(':') {
                path = format!(".{}", path.split_at(1).1);
            }
            let path = format!("${path}");
            let new_expr = ASTExpr::Function(Function {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    "variant_element".to_string(),
                ))]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*value.clone())),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ASTExpr::Value(
                            ValueWithSpan {
                                value: Value::SingleQuotedString(path),
                                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                            },
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ASTExpr::Value(
                            ValueWithSpan {
                                value: Value::Boolean(true),
                                span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                            },
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
    let _ = stmt.visit(&mut VariantElementVisitor::new());
}

// For unit tests see udfs/variant_element.rs
