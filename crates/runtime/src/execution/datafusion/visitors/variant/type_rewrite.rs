// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{DataType, Statement, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct ArrayObjectToBinaryVisitor;

impl ArrayObjectToBinaryVisitor {
    pub fn new() -> Self {
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
    stmt.visit(&mut ArrayObjectToBinaryVisitor::new());
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
