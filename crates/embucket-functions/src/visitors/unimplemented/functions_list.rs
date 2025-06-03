use std::collections::HashMap;
use std::sync::OnceLock;

// Include the generated functions
use crate::visitors::unimplemented::FunctionInfo;
use crate::visitors::unimplemented::generated_snowflake_functions::{
    ACCOUNT_FUNCTIONS, AGGREGATE_FUNCTIONS, BITWISE_FUNCTIONS, CONDITIONAL_FUNCTIONS,
    CONTEXT_FUNCTIONS, CONVERSION_FUNCTIONS, DATA_METRIC_FUNCTIONS, DATA_QUALITY_FUNCTIONS,
    DATETIME_FUNCTIONS, DIFFERENTIAL_PRIVACY_FUNCTIONS, ENCRYPTION_FUNCTIONS, FILE_FUNCTIONS,
    GENERATION_FUNCTIONS, GEOSPATIAL_FUNCTIONS, HASH_FUNCTIONS, ICEBERG_FUNCTIONS,
    INFORMATION_SCHEMA_FUNCTIONS, METADATA_FUNCTIONS, NOTIFICATION_FUNCTIONS, NUMERIC_FUNCTIONS,
    SEMISTRUCTURED_FUNCTIONS, STRING_BINARY_FUNCTIONS, SYSTEM_FUNCTIONS, TABLE_FUNCTIONS,
    VECTOR_FUNCTIONS, WINDOW_FUNCTIONS,
};

/// Organizes Snowflake functions in a single registry
pub struct SnowflakeFunctions {
    pub functions: HashMap<&'static str, FunctionInfo>,
}

/// Singleton instance of `SnowflakeFunctions`
static SNOWFLAKE_FUNCTIONS: OnceLock<SnowflakeFunctions> = OnceLock::new();

impl Default for SnowflakeFunctions {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to convert const arrays to `HashMaps`
fn build_hashmap_from_array(
    functions: &'static [(&'static str, FunctionInfo)],
) -> HashMap<&'static str, FunctionInfo> {
    functions
        .iter()
        .map(|(name, info)| (*name, info.clone()))
        .collect()
}

impl SnowflakeFunctions {
    fn new() -> Self {
        let mut functions = HashMap::new();

        // Combine all categories into a single HashMap
        functions.extend(build_hashmap_from_array(NUMERIC_FUNCTIONS));
        functions.extend(build_hashmap_from_array(STRING_BINARY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(DATETIME_FUNCTIONS));
        functions.extend(build_hashmap_from_array(SEMISTRUCTURED_FUNCTIONS));
        functions.extend(build_hashmap_from_array(AGGREGATE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(WINDOW_FUNCTIONS));
        functions.extend(build_hashmap_from_array(CONDITIONAL_FUNCTIONS));
        functions.extend(build_hashmap_from_array(CONVERSION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(CONTEXT_FUNCTIONS));
        functions.extend(build_hashmap_from_array(SYSTEM_FUNCTIONS));
        functions.extend(build_hashmap_from_array(GEOSPATIAL_FUNCTIONS));
        functions.extend(build_hashmap_from_array(TABLE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(INFORMATION_SCHEMA_FUNCTIONS));
        functions.extend(build_hashmap_from_array(BITWISE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(HASH_FUNCTIONS));
        functions.extend(build_hashmap_from_array(ENCRYPTION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(FILE_FUNCTIONS));
        functions.extend(build_hashmap_from_array(NOTIFICATION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(GENERATION_FUNCTIONS));
        functions.extend(build_hashmap_from_array(VECTOR_FUNCTIONS));
        functions.extend(build_hashmap_from_array(DIFFERENTIAL_PRIVACY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(DATA_METRIC_FUNCTIONS));
        functions.extend(build_hashmap_from_array(DATA_QUALITY_FUNCTIONS));
        functions.extend(build_hashmap_from_array(METADATA_FUNCTIONS));
        functions.extend(build_hashmap_from_array(ACCOUNT_FUNCTIONS));
        functions.extend(build_hashmap_from_array(ICEBERG_FUNCTIONS));

        Self { functions }
    }

    /// Check if a function exists in the registry
    #[must_use]
    pub fn check_function_exists(&self, function_name: &str) -> bool {
        self.functions.contains_key(function_name)
    }

    /// Check if a function is unimplemented (exists in our registry)
    #[must_use]
    pub fn is_unimplemented(&self, function_name: &str) -> bool {
        let function_name_upper = function_name.to_uppercase();
        self.check_function_exists(function_name_upper.as_str())
    }

    /// Get function information
    #[must_use]
    pub fn get_function_info(&self, function_name: &str) -> Option<&FunctionInfo> {
        self.functions.get(function_name)
    }

    /// Get total function count
    #[must_use]
    pub fn total_function_count(&self) -> usize {
        self.functions.len()
    }

    /// Get all function names
    #[must_use]
    pub fn get_all_function_names(&self) -> Vec<&str> {
        self.functions.keys().copied().collect()
    }
}

pub fn get_snowflake_functions() -> &'static SnowflakeFunctions {
    SNOWFLAKE_FUNCTIONS.get_or_init(SnowflakeFunctions::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_valid_sql_identifier(name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        let chars: Vec<char> = name.chars().collect();

        let first_char = chars[0];
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return false;
        }

        for &ch in &chars[1..] {
            if !ch.is_ascii_alphanumeric()
                && ch != '_'
                && ch != '$'
                && !(ch == ' ' && name.contains("LIKE"))
            {
                return false;
            }
        }

        true
    }

    #[test]
    fn test_all_function_names_are_valid() {
        let functions = get_snowflake_functions();
        let all_names = functions.get_all_function_names();

        let mut invalid_names = Vec::new();

        for &name in &all_names {
            if !is_valid_sql_identifier(name) {
                invalid_names.push(name);
            }
        }
        assert!(
            invalid_names.is_empty(),
            "Found invalid function names: \n {}",
            invalid_names.join("\n")
        );
    }
}
