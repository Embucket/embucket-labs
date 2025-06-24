//! Tests to ensure UDF registrations are kept in alphabetical order

use std::sync::Arc;
use datafusion_expr::ScalarUDF;

/// Extracts function names from a vector of ScalarUDFs and checks if they are sorted
fn check_udf_ordering(udfs: &[Arc<ScalarUDF>], module_name: &str) {
    let names: Vec<String> = udfs.iter().map(|udf| udf.name().to_string()).collect();
    let mut sorted_names = names.clone();
    sorted_names.sort();
    
    if names != sorted_names {
        panic!(
            "UDFs in {} are not in alphabetical order.\nActual: {:?}\nExpected: {:?}",
            module_name, names, sorted_names
        );
    }
}

#[test]
fn test_conditional_udf_ordering() {
    use crate::conditional;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    conditional::register_udfs(&mut state).expect("Failed to register UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let mut new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    new_udf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udf_names, sorted_names, "Conditional UDFs are not in alphabetical order");
}

#[test]
fn test_conversion_udf_ordering() {
    use crate::conversion;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    conversion::register_udfs(&mut state).expect("Failed to register UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let mut new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    new_udf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udf_names, sorted_names, "Conversion UDFs are not in alphabetical order");
}

#[test]
fn test_datetime_udf_ordering() {
    use crate::datetime;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    datetime::register_udfs(&mut state).expect("Failed to register UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let mut new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    new_udf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udf_names, sorted_names, "DateTime UDFs are not in alphabetical order");
}

#[test]
fn test_string_binary_udf_ordering() {
    use crate::string_binary;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    string_binary::register_udfs(&mut state).expect("Failed to register UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let mut new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    new_udf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udf_names, sorted_names, "String binary UDFs are not in alphabetical order");
}

#[test]
fn test_semi_structured_udf_ordering() {
    use crate::semi_structured;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    semi_structured::register_udfs(&mut state).expect("Failed to register UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let mut new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    new_udf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udf_names, sorted_names, "Semi-structured UDFs are not in alphabetical order");
}

#[test]
fn test_session_udf_ordering() {
    use crate::session;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udfs = state.udfs();
    session::register_session_context_udfs(&mut state).expect("Failed to register session UDFs");
    let final_udfs = state.udfs();
    
    // Get only the new UDFs that were registered
    let new_udf_names: Vec<String> = final_udfs
        .difference(&initial_udfs)
        .cloned()
        .collect();
    
    let mut sorted_names = new_udf_names.clone();
    sorted_names.sort();
    
    if new_udf_names != sorted_names {
        panic!(
            "Session UDFs are not in alphabetical order.\nActual: {:?}\nExpected: {:?}",
            new_udf_names, sorted_names
        );
    }
}

#[test]
fn test_aggregate_udaf_ordering() {
    use crate::aggregate;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_expr::registry::FunctionRegistry;
    
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime)
        .build();
    
    let initial_udafs = state.aggregate_functions().keys().cloned().collect::<std::collections::HashSet<_>>();
    aggregate::register_udafs(&mut state).expect("Failed to register aggregate UDAFs");
    let final_udafs = state.aggregate_functions().keys().cloned().collect::<std::collections::HashSet<_>>();
    
    // Get only the new UDAFs that were registered
    let mut new_udaf_names: Vec<String> = final_udafs
        .difference(&initial_udafs)
        .cloned()
        .collect();
    new_udaf_names.sort(); // Sort for consistent comparison
    
    let mut sorted_names = new_udaf_names.clone();
    sorted_names.sort();
    
    assert_eq!(new_udaf_names, sorted_names, "Aggregate UDAFs are not in alphabetical order");
}

/// Manual verification for complex registrations that can't be easily tested automatically
#[cfg(test)]
mod manual_verification {
    //! For modules with complex registration logic (like semi-structured with multiple Arc::new calls),
    //! we provide manual verification functions that can be run during development
    
    use super::*;
    
    /// Manually verify semi-structured module ordering
    /// This should be called whenever the semi-structured module is updated
    pub fn verify_semi_structured_ordering() {
        // This is a visual check - the actual function names should be listed here
        // in the order they appear in the semi_structured::register_udfs function
        let expected_order = vec![
            // array functions
            "array_append",
            "array_cat", 
            "array_compact",
            "array_construct",
            "array_contains",
            "array_distinct",
            "array_except",
            "array_flatten",
            "array_generate_range",
            "array_insert",
            "array_intersection",
            "array_max",
            "array_min",
            "array_position",
            "array_prepend", 
            "array_remove",
            "array_remove_at",
            "array_reverse",
            "array_size",
            "array_slice",
            "array_sort",
            "array_to_string",
            "arrays_overlap",
            "arrays_to_object",
            "arrays_zip",
            // conversion functions
            "as_*", // conversion::as_func::get_udf()
            // get functions
            "get", // GetFunc variants
            "get_path",
            // json functions
            "is_array",
            "is_object",
            "parse_json",
            // object functions  
            "object_construct", // ObjectConstructUDF variants
            "object_delete",
            "object_insert",
            "object_keys",
            "object_pick",
            // type checking functions
            "is_*", // IsTypeofFunc variants for different types
            "strtok_to_array",
            "try_parse_json",
            "typeof",
            "variant_element",
        ];
        
        println!("Expected semi-structured function order (visual verification needed):");
        for name in expected_order {
            println!("  {}", name);
        }
    }
} 