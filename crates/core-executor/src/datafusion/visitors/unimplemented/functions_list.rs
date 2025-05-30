use std::collections::HashMap;

// Include the generated functions
use crate::datafusion::visitors::unimplemented::generated_snowflake_functions::*;
use crate::datafusion::visitors::unimplemented::FunctionInfo;

/// Organizes Snowflake functions in a single registry
pub struct SnowflakeFunctions {
    pub functions: HashMap<&'static str, FunctionInfo>,
}

impl Default for SnowflakeFunctions {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to convert const arrays to HashMaps
fn build_hashmap_from_array(functions: &'static [(&'static str, FunctionInfo)]) -> HashMap<&'static str, FunctionInfo> {
    functions.iter().map(|(name, info)| (*name, info.clone())).collect()
}

impl SnowflakeFunctions {
    pub fn new() -> Self {
        let mut functions = HashMap::new();

        // Combine all categories into a single HashMap
        functions.extend(build_hashmap_from_array(&NUMERIC_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&STRING_BINARY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&DATETIME_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&SEMISTRUCTURED_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&AGGREGATE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&WINDOW_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&CONDITIONAL_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&CONVERSION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&CONTEXT_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&SYSTEM_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&GEOSPATIAL_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&TABLE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&INFORMATION_SCHEMA_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&BITWISE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&HASH_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&ENCRYPTION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&FILE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&NOTIFICATION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&GENERATION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&VECTOR_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&DIFFERENTIAL_PRIVACY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&DATA_METRIC_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&DATA_QUALITY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&METADATA_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&ACCOUNT_FUNCTIONS));
        functions.extend(build_hashmap_from_array(&ICEBERG_FUNCTIONS));

        Self { functions }
    }

    /// Check if a function exists in the registry
    pub fn check_function_exists(&self, function_name: &str) -> bool {
        self.functions.contains_key(function_name)
    }

    /// Check if a function is unimplemented (exists in our registry)
    pub fn is_unimplemented(&self, function_name: &str) -> bool {
        let function_name_upper = function_name.to_uppercase();
        self.check_function_exists(function_name_upper.as_str())
    }

    /// Get function information
    pub fn get_function_info(&self, function_name: &str) -> Option<&FunctionInfo> {
        self.functions.get(function_name)
    }

    /// Get total function count
    pub fn total_function_count(&self) -> usize {
        self.functions.len()
    }

    /// Get all function names
    pub fn get_all_function_names(&self) -> Vec<&str> {
        self.functions.keys().copied().collect()
    }
}

/// Create a lazy static instance of the functions registry
pub fn get_snowflake_functions() -> SnowflakeFunctions {
    SnowflakeFunctions::new()
}