use datafusion::logical_expr::sqlparser::tokenizer::{Location, Span};
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgumentList, FunctionArguments, Ident, JsonPath,
    JsonPathElem, ObjectName, ObjectNamePart, Statement, Value, ValueWithSpan, VisitorMut,
};

#[derive(Debug)]
pub struct VariantArrayAggRewriter;

impl VisitorMut for VariantArrayAggRewriter {
    type Break = bool;

    fn post_visit_expr(
        &mut self,
        expr: &mut datafusion_expr::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion_expr::sqlparser::ast::Expr::Function(Function { name, .. }) = expr {
            if let Some(part) = name.0.last() {
                if part.as_ident().is_some_and(|i| i.value == "array_agg") {
                    let wrapped_function = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                            "array_construct".to_string(),
                        ))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![FunctionArg::Unnamed(
                                datafusion_expr::sqlparser::ast::FunctionArgExpr::Expr(
                                    expr.clone(),
                                ),
                            )],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Vec::default(),
                    };
                    let fn_call_expr =
                        datafusion_expr::sqlparser::ast::Expr::Function(wrapped_function);

                    let index_expr = Expr::JsonAccess {
                        value: Box::new(fn_call_expr),
                        path: JsonPath {
                            path: vec![JsonPathElem::Bracket {
                                key: Expr::Value(ValueWithSpan {
                                    value: Value::Number("0".to_string(), false),
                                    span: Span::new(Location::new(0, 0), Location::new(0, 0)),
                                }),
                            }],
                        },
                    };

                    *expr = index_expr;
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut VariantArrayAggRewriter {});
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use crate::semi_structured::variant::variant_element::VariantArrayElementUDF;
    use crate::semi_structured::variant::visitors::variant_element;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_agg_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct UDF

        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(VariantArrayElementUDF::new()));

        // Create table and insert data
        let create_sql = "CREATE TABLE test_table (id INT, val INT)";
        let mut create_stmt = ctx.state().sql_to_statement(create_sql, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = create_stmt {
            visit(stmt);
        }
        ctx.sql(&create_stmt.to_string()).await?.collect().await?;

        let insert_sql = "INSERT INTO test_table VALUES (1, 1), (1, 2), (1, 3)";
        ctx.sql(insert_sql).await?.collect().await?;

        // Test array_agg rewrite by validating JSON output
        let sql = "SELECT array_agg(val) as json_arr FROM test_table GROUP BY id";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
            variant_element::visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+----------+",
                "| json_arr |",
                "+----------+",
                "| [1,2,3]  |",
                "+----------+",
            ],
            &result
        );

        Ok(())
    }
}
