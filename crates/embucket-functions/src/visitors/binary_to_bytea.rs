use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{DataType, Statement, VisitorMut};
use std::ops::ControlFlow;

/// Visitor that converts BINARY and VARBINARY types to BYTEA in CREATE TABLE statements
///
/// This allows `DataFusion` to support standard SQL BINARY types by converting them to
/// the supported BYTEA type, which maps to `DataType::Binary` internally.
#[derive(Debug, Default)]
pub struct BinaryToBytea;

impl BinaryToBytea {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut for BinaryToBytea {
    type Break = ();

    fn post_visit_statement(&mut self, stmt: &mut Statement) -> ControlFlow<Self::Break> {
        if let Statement::CreateTable(create_table) = stmt {
            for column in &mut create_table.columns {
                // Convert BINARY(n) and VARBINARY(n) to BYTEA
                match &column.data_type {
                    DataType::Binary(_) | DataType::Varbinary(_) => {
                        column.data_type = DataType::Bytea;
                    }
                    _ => {}
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut BinaryToBytea::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_common::Result as DFResult;

    #[tokio::test]
    async fn test_binary_to_bytea_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with BINARY type
        let sql = "CREATE TABLE test_table (id INT, data BINARY(16))";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, data BYTEA)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_varbinary_to_bytea_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with VARBINARY type
        let sql = "CREATE TABLE test_table (id INT, data VARBINARY(255))";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, data BYTEA)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_types_rewrite() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test table creation with mixed types including BINARY
        let sql = "CREATE TABLE test_table (id INT, name VARCHAR(50), data BINARY(32), blob VARBINARY(1000))";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, name VARCHAR(50), data BYTEA, blob BYTEA)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_non_binary_types_without_size() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Test that non-binary types are left unchanged
        let sql = "CREATE TABLE test_table (id INT, name BINARY, created_at VARBINARY)";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;

        if let DFStatement::Statement(ref mut stmt) = statement {
            visit(stmt);
        }

        assert_eq!(
            statement.to_string(),
            "CREATE TABLE test_table (id INT, name BYTEA, created_at BYTEA)"
        );

        Ok(())
    }
}
