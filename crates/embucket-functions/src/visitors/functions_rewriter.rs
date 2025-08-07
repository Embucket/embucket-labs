use datafusion::sql::sqlparser::ast::ValueWithSpan;
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
                "dateadd" | "date_add" | "datediff" | "date_diff" => {
                    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args
                        && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(ident))) =
                            args.iter_mut().next()
                        && let Expr::Identifier(Ident { value, .. }) = ident
                    {
                        *ident = Expr::Value(SingleQuotedString(value.clone()).into());
                    }
                    func_name
                }
                "variance" | "variance_samp" => "var_samp",
                "variance_pop" => "var_pop",
                "date" => "to_date",
                //regexp_ udfs need `\\` in the pattern for regex, but when converting to the logical plan, it gets removed,
                // the only way to make it stay is to make it `\\\\` or maybe use another type?
                // Escaped blah blah String from postgres types may work, but differently
                fn_name if fn_name.starts_with("regexp_") => {
                    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args
                        && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(pattern))) =
                            args.get_mut(1)
                        //second arg is pattern for regex
                            && let Expr::Value(ValueWithSpan {
                                value: SingleQuotedString(value),
                                span,
                            }) = pattern
                    {
                        let value = value.replace('\\', "\\\\");
                        *pattern = Expr::Value(ValueWithSpan {
                            value: SingleQuotedString(value),
                            span: *span,
                        });
                    }
                    match func_name {
                        "rlike" => "regexp_like",
                        "regexp_extract_all" => "regexp_substr_all",
                        _ => func_name,
                    }
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
