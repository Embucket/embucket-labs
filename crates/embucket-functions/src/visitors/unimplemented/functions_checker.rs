use super::functions_list::get_snowflake_functions;
use datafusion_expr::sqlparser::ast::{Expr, Statement, VisitMut, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, Clone)]
pub struct UnimplementedFunctionError {
    pub function_name: String,
    pub details: Option<String>,
}

impl std::fmt::Display for UnimplementedFunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(details) = &self.details {
            if !details.is_empty() {
                return write!(
                    f,
                    "Function '{}' is not implemented yet. Details: {}",
                    self.function_name, details
                );
            }
        }

        write!(
            f,
            "Function '{}' is not implemented yet",
            self.function_name
        )
    }
}

impl std::error::Error for UnimplementedFunctionError {}

pub struct UnimplementedFunctionsChecker;

impl VisitorMut for UnimplementedFunctionsChecker {
    type Break = UnimplementedFunctionError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            let func_name = func.name.to_string().to_uppercase();
            let snowflake_functions = get_snowflake_functions();

            if snowflake_functions.is_unimplemented(&func_name) {
                let details = snowflake_functions
                    .get_function_info(&func_name)
                    .and_then(|info| {
                        info.get_preferred_url()
                            .map(std::string::ToString::to_string)
                    });

                return ControlFlow::Break(UnimplementedFunctionError {
                    function_name: func_name.to_lowercase(),
                    details,
                });
            }
        }

        ControlFlow::Continue(())
    }
}

/// Check if a statement contains any unimplemented functions
pub fn check_unimplemented_functions(
    stmt: &mut Statement,
) -> Result<(), UnimplementedFunctionError> {
    match stmt.visit(&mut UnimplementedFunctionsChecker) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

pub fn visit(stmt: &mut Statement) -> Result<(), UnimplementedFunctionError> {
    match stmt.visit(&mut UnimplementedFunctionsChecker) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::sqlparser::{dialect::SnowflakeDialect, parser::Parser};

    #[test]
    fn test_implemented_function_passes() {
        let sql = "SELECT COUNT(*), SUM(column1), AVG(column2) FROM table1";
        let mut statements =
            Parser::parse_sql(&SnowflakeDialect {}, sql).expect("Failed to parse SQL in test");

        let result = check_unimplemented_functions(&mut statements[0]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_nested_unimplemented_function() {
        let sql = "SELECT COUNT(*) FROM table1 WHERE column1 = CHECK_JSON(data)";
        let mut statements =
            Parser::parse_sql(&SnowflakeDialect {}, sql).expect("Failed to parse SQL in test");

        let result = check_unimplemented_functions(&mut statements[0]);
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e.function_name, "check_json");
        }
    }

    #[test]
    fn test_error_message_formatting() {
        let error_without_details = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: None,
        };
        assert_eq!(
            error_without_details.to_string(),
            "Function 'test_func' is not implemented yet"
        );

        let error_with_empty_details = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: Some(String::new()),
        };
        assert_eq!(
            error_with_empty_details.to_string(),
            "Function 'test_func' is not implemented yet"
        );

        let error_with_issue_url = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: Some("https://github.com/embucket/control-plane-v2/issues/123".to_string()),
        };
        assert_eq!(
            error_with_issue_url.to_string(),
            "Function 'test_func' is not implemented yet. Details: https://github.com/embucket/control-plane-v2/issues/123"
        );

        let error_with_docs_url = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: Some(
                "https://docs.snowflake.com/en/sql-reference/functions/test_func".to_string(),
            ),
        };
        assert_eq!(
            error_with_docs_url.to_string(),
            "Function 'test_func' is not implemented yet. Details: https://docs.snowflake.com/en/sql-reference/functions/test_func"
        );
    }
}
