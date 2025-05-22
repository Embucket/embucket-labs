use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{DataType, Statement, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct ArrayObjectToBinaryVisitor;

impl ArrayObjectToBinaryVisitor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut for ArrayObjectToBinaryVisitor {
    type Break = ();

    fn post_visit_statement(&mut self, stmt: &mut Statement) -> ControlFlow<Self::Break> {
        if let Statement::CreateTable(create_table) = stmt {
            for column in &mut create_table.columns {
                if let DataType::Array(_) = &column.data_type {
                    column.data_type = DataType::String(None);
                }
                if let DataType::Custom(_, _) = &column.data_type {
                    if column.data_type.to_string() == "OBJECT" {
                        column.data_type = DataType::String(None);
                    }
                    if column.data_type.to_string() == "VARIANT" {
                        column.data_type = DataType::String(None);
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut ArrayObjectToBinaryVisitor::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;

    #[tokio::test]
    async fn test_array_to_binary_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with Array type
        let sql = "CREATE TABLE test_table (id INT, arr ARRAY)";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, arr STRING)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_to_binary_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with Array type
        let sql = "CREATE TABLE test_table (id INT, obj OBJECT)";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, obj STRING)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_variant_to_binary_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with Array type
        let sql = "CREATE TABLE test_table (id INT, variant VARIANT)";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, variant STRING)"
        );

        Ok(())
    }
}
