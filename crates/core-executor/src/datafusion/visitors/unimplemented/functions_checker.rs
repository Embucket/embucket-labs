use datafusion_expr::sqlparser::ast::{
    Expr, Statement, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;
use super::functions_list::get_snowflake_functions;


#[derive(Debug, Clone)]
pub struct UnimplementedFunctionError {
    pub function_name: String,
    pub details: Option<String>,
}

impl std::fmt::Display for UnimplementedFunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(details) = &self.details {
            if !details.is_empty() {
                return write!(f, "Function '{}' is not implemented yet. Details: {}", self.function_name, details);
            }
        } 

        return write!(f, "Function '{}' is not implemented yet", self.function_name);
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
                return ControlFlow::Break(UnimplementedFunctionError {
                    function_name: func_name.to_lowercase(),
                    details: Some(snowflake_functions.get_function_info(&func_name).unwrap().description.clone()),
                });
            }
        }
        
        ControlFlow::Continue(())
    }
}

/// Check if a statement contains any unimplemented functions
pub fn check_unimplemented_functions(stmt: &mut Statement) -> Result<(), UnimplementedFunctionError> {
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
    use datafusion_expr::sqlparser::{dialect::GenericDialect, parser::Parser};
    
    #[test]
    fn test_unimplemented_function_detection() {
        let sql = "SELECT MD5(column1) FROM table1";
        let mut statements = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
        
        let result = check_unimplemented_functions(&mut statements[0]);
        assert!(result.is_err());
        
        if let Err(e) = result {
            assert_eq!(e.function_name, "md5");
            assert_eq!(e.to_string(), "Function 'md5' is not implemented yet");
        }
    }
    
    #[test]
    fn test_implemented_function_passes() {
        // Test with functions that should not be in our Snowflake registry
        let sql = "SELECT COUNT(*), SUM(column1), AVG(column2) FROM table1";
        let mut statements = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
        
        let result = check_unimplemented_functions(&mut statements[0]);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_nested_unimplemented_function() {
        let sql = "SELECT COUNT(*) FROM table1 WHERE column1 = PARSE_JSON(data)";
        let mut statements = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
        
        let result = check_unimplemented_functions(&mut statements[0]);
        assert!(result.is_err());
        
        if let Err(e) = result {
            assert_eq!(e.function_name, "parse_json");
        }
    }
    
    #[test]
    fn test_snowflake_specific_functions() {
        // Test some functions that should be in our Snowflake registry
        let test_cases = vec![
            ("SELECT APPROX_COUNT_DISTINCT(column1) FROM table1", "approx_count_distinct"),
            ("SELECT REGEXP_COUNT('hello', 'l') FROM dual", "regexp_count"),
            ("SELECT TO_VARCHAR(42) FROM dual", "to_varchar"),
        ];
        
        for (sql, expected_function) in test_cases {
            let mut statements = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
            let result = check_unimplemented_functions(&mut statements[0]);
            
            assert!(result.is_err(), "Expected {} to be unimplemented", expected_function);
            if let Err(e) = result {
                assert_eq!(e.function_name, expected_function);
            }
        }
    }
    
    #[test]
    fn test_error_message_formatting() {
        // Test error message without details
        let error_without_details = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: None,
        };
        assert_eq!(error_without_details.to_string(), "Function 'test_func' is not implemented yet");
        
        // Test error message with empty details
        let error_with_empty_details = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: Some("".to_string()),
        };
        assert_eq!(error_with_empty_details.to_string(), "Function 'test_func' is not implemented yet");
        
        // Test error message with actual details
        let error_with_details = UnimplementedFunctionError {
            function_name: "test_func".to_string(),
            details: Some("This function requires special handling".to_string()),
        };
        assert_eq!(error_with_details.to_string(), "Function 'test_func' is not implemented yet. Details: This function requires special handling");
    }
} 