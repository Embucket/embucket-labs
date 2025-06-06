use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, Statement, VisitorMut,
};

#[derive(Debug)]
pub struct ArrayConstructCompactRewriter;

impl VisitorMut for ArrayConstructCompactRewriter {
    type Break = bool;

    fn post_visit_expr(
        &mut self,
        expr: &mut datafusion_expr::sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if let datafusion_expr::sqlparser::ast::Expr::Function(Function { name, args, .. }) = expr {
            if let Some(part) = name.0.last() {
                if part
                    .as_ident()
                    .is_some_and(|i| i.value == "array_construct_compact")
                {
                    // Create the inner array_construct function
                    let array_construct = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                            "array_construct".to_string(),
                        ))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: args.clone(),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Vec::default(),
                    };

                    // Create the outer array_compact function that wraps array_construct
                    let array_compact = Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                            "array_compact".to_string(),
                        ))]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![FunctionArg::Unnamed(
                                datafusion_expr::sqlparser::ast::FunctionArgExpr::Expr(
                                    Expr::Function(array_construct),
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

                    *expr = Expr::Function(array_compact);
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut ArrayConstructCompactRewriter {});
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_compact::ArrayCompactUDF;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_construct_compact_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct and array_compact UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(ArrayCompactUDF::new()));

        // Test array_construct_compact rewrite
        let sql = "SELECT array_construct_compact(1, NULL, 2, NULL, 3) as compact_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| compact_arr |",
                "+-------------+",
                "| [1,2,3]     |",
                "+-------------+",
            ],
            &result
        );

        Ok(())
    }
}
