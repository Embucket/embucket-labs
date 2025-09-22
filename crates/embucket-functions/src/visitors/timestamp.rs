use datafusion::logical_expr::sqlparser::ast::{DataType, ObjectNamePart, TimezoneInfo};
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, VisitorMut,
};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct TimestampVisitor {}

impl VisitorMut for TimestampVisitor {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> ControlFlow<Self::Break> {
        *expr = match expr.clone() {
            ASTExpr::Cast {
                expr: cast_expr,
                data_type,
                ..
            } => match data_type {
                DataType::Timestamp(_, TimezoneInfo::None) => {
                    func("to_timestamp".to_string(), *cast_expr)
                }
                // Handle explicit NTZ variant if available; other flavors fall through to Custom
                DataType::TimestampNtz => func("to_timestamp_ntz".to_string(), *cast_expr),
                DataType::Custom(ObjectName(v), ..) => match &v[0] {
                    ObjectNamePart::Identifier(ident) => {
                        let name = ident.value.to_ascii_lowercase();
                        match name.as_str() {
                            "timestamp_tz" | "timestamp_ntz" | "timestamp_ltz" => {
                                func(format!("to_{name}"), *cast_expr)
                            }
                            _ => expr.clone(),
                        }
                    }
                    ObjectNamePart::Function(_) => expr.clone(),
                },
                _ => expr.clone(),
            },
            other => other,
        };

        ControlFlow::Continue(())
    }
}

fn func(name: String, arg: ASTExpr) -> ASTExpr {
    ASTExpr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut TimestampVisitor {});
}
