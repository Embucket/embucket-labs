use datafusion_expr::sqlparser::ast::Value::SingleQuotedString;
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    Statement, VisitorMut,
};

#[derive(Debug, Default)]
pub struct FunctionsRewriter {}

impl VisitorMut for FunctionsRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name_string = func.name.clone().to_string().to_lowercase();
            let func_name = func_name_string.as_str();
            let args = &mut func.args;
            let name = match func_name {
                "year" | "day" | "dayofmonth" | "dayofweek" | "dayofweekiso" | "dayofyear"
                | "week" | "weekofyear" | "weekiso" | "month" | "quarter" | "hour" | "minute"
                | "second" => {
                    if let FunctionArguments::List(arg_list) = args {
                        let arg = match func_name {
                            "year" | "quarter" | "month" | "week" | "day" | "hour" | "minute"
                            | "second" => func_name,
                            "dayofyear" => "doy",
                            "dayofweek" => "dow",
                            "dayofmonth" => "day",
                            "weekofyear" => "week",
                            _ => "unknown",
                        };
                        arg_list.args.insert(
                            0,
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                SingleQuotedString(arg.to_string()).into(),
                            ))),
                        );
                    }
                    "date_part"
                }
                "dateadd" | "date_add" | "datediff" | "date_diff" => {
                    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args {
                        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(ident))) =
                            args.iter_mut().next()
                        {
                            if let Expr::Identifier(Ident { value, .. }) = ident {
                                *ident = Expr::Value(SingleQuotedString(value.clone()).into());
                            }
                        }
                    }
                    func_name
                }
                _ => func_name,
            };
            func.name = ObjectName::from(vec![Ident::new(name)]);
        }
        std::ops::ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut FunctionsRewriter {});
}
