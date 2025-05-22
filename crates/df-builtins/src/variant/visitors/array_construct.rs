use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr as ASTExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Statement, VisitorMut,
};

#[derive(Debug, Default)]
pub struct ArrayConstructVisitor;

impl ArrayConstructVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut for ArrayConstructVisitor {
    fn post_visit_expr(&mut self, expr: &mut ASTExpr) -> std::ops::ControlFlow<Self::Break> {
        if let ASTExpr::Array(elements) = expr {
            let args = elements
                .elem
                .iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
                .collect();

            let new_expr = ASTExpr::Function(Function {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    "array_construct",
                ))]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args,
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
    let _ = stmt.visit(&mut ArrayConstructVisitor::new());
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::variant::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_construct_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();
        // Register array_construct UDF
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));

        // Test simple array construction
        let sql = "SELECT [1, 2, 3] as arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| arr     |",
                "+---------+",
                "| [1,2,3] |",
                "+---------+",
            ],
            &result
        );

        // Test array with mixed types
        let sql = "SELECT [1, 'test', null] as mixed_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }
        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------------+",
                "| mixed_arr       |",
                "+-----------------+",
                "| [1,\"test\",null] |",
                "+-----------------+",
            ],
            &result
        );

        // Test nested arrays
        let sql = "SELECT [[1, 2], [3, 4]] as nested_arr";
        let mut stmt = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let DFStatement::Statement(ref mut s) = stmt {
            visit(s);
        }

        let result = ctx.sql(&stmt.to_string()).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| nested_arr    |",
                "+---------------+",
                "| [[1,2],[3,4]] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
