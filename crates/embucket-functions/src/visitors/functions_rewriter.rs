use datafusion::sql::sqlparser::ast::ValueWithSpan;
use datafusion_expr::sqlparser::ast::Value::SingleQuotedString;
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    Statement, Value, VisitorMut,
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
                "dateadd" | "date_add" | "datediff" | "date_diff" | "date_part"
                | "timestampdiff" | "timestamp_diff" | "time_diff" | "timediff" => {
                    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args
                        && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(ident))) =
                            args.iter_mut().next()
                    {
                        match ident {
                            Expr::Identifier(Ident { value, .. }) => {
                                let transformed_value = match value.as_str() {
                                    "epoch_second" | "epoch_seconds" => "epoch".to_string(),
                                    _ => value.clone(),
                                };
                                *ident = Expr::Value(SingleQuotedString(transformed_value).into());
                            }

                            Expr::Value(ValueWithSpan {
                                value: Value::SingleQuotedString(value),
                                ..
                            }) => {
                                // Handle already quoted strings
                                let transformed_value = match value.as_str() {
                                    "epoch_second" | "epoch_seconds" => "epoch".to_string(),
                                    _ => value.clone(),
                                };
                                *value = transformed_value;
                            }
                            Expr::Value(ValueWithSpan {
                                value: Value::DoubleQuotedString(value),
                                ..
                            }) => {
                                // Handle already quoted strings
                                let transformed_value = match value.as_str() {
                                    "epoch_second" | "epoch_seconds" => "epoch".to_string(),
                                    _ => value.clone(),
                                };
                                *value = transformed_value;
                            }
                            _ => {}
                        }
                    }
                    func_name
                }
                "variance" | "variance_samp" => "var_samp",
                "variance_pop" => "var_pop",
                "grouping_id" => "grouping",
                "to_char" => {
                    rewrite_date_format(args);
                    func_name
                }
                "date" => "to_date",
                //regexp_ udfs need `\\` in the pattern for regex, but when converting to the logical plan, it gets removed,
                // the only way to make it stay is to make it `\\\\` or maybe use another type?
                // Escaped blah blah String from postgres types may work, but differently
                fn_name if fn_name.starts_with("regexp_") || fn_name == "rlike" => {
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
                    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args
                        && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(replacement))) =
                        args.get_mut(2)
                        //third arg may be a replacement
                        && let Expr::Value(ValueWithSpan {
                                               value: SingleQuotedString(value),
                                               span,
                                           }) = replacement
                    {
                        let value = if let Ok(regex) = regex::Regex::new(r"\\(\d)") {
                            regex
                                .replace_all(value, |caps: &regex::Captures| {
                                    format!("${}", &caps[1]) // Replace with `$number`.
                                })
                                .to_string()
                        } else {
                            (*value).to_string()
                        };
                        *replacement = Expr::Value(ValueWithSpan {
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

fn rewrite_date_format(args: &mut FunctionArguments) {
    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = args
        && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(format_expr))) = args.get_mut(1)
        && let Expr::Value(ValueWithSpan {
            value: SingleQuotedString(fmt),
            span: _,
        }) = format_expr
    {
        // Replace common date format specifiers with DataFusion's format
        // https://docs.snowflake.com/en/sql-reference/functions-conversion#label-date-time-format-conversion
        // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        *fmt = fmt
            .replace("YYYY", "%Y")
            .replace("MM", "%m")
            .replace("DD", "%d")
            .replace("HH24", "%H")
            .replace("MI", "%M")
            .replace("SS", "%S");
    }
}
