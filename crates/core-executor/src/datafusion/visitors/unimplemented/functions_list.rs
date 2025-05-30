use std::collections::HashMap;

// Include the generated functions
use crate::datafusion::visitors::unimplemented::generated_snowflake_functions::*;
use crate::datafusion::visitors::unimplemented::FunctionInfo;

/// Organizes Snowflake functions by categories
pub struct SnowflakeFunctions {
    pub numeric_functions: HashMap<&'static str, FunctionInfo>,
    pub string_binary_functions: HashMap<&'static str, FunctionInfo>,
    pub date_time_functions: HashMap<&'static str, FunctionInfo>,
    pub semi_structured_functions: HashMap<&'static str, FunctionInfo>,
    pub aggregate_functions: HashMap<&'static str, FunctionInfo>,
    pub window_functions: HashMap<&'static str, FunctionInfo>,
    pub conditional_functions: HashMap<&'static str, FunctionInfo>,
    pub conversion_functions: HashMap<&'static str, FunctionInfo>,
    pub context_functions: HashMap<&'static str, FunctionInfo>,
    pub system_functions: HashMap<&'static str, FunctionInfo>,
    pub geospatial_functions: HashMap<&'static str, FunctionInfo>,
    pub table_functions: HashMap<&'static str, FunctionInfo>,
    pub information_schema_functions: HashMap<&'static str, FunctionInfo>,
    pub bitwise_functions: HashMap<&'static str, FunctionInfo>,
    pub hash_functions: HashMap<&'static str, FunctionInfo>,
    pub encryption_functions: HashMap<&'static str, FunctionInfo>,
    pub file_functions: HashMap<&'static str, FunctionInfo>,
    pub notification_functions: HashMap<&'static str, FunctionInfo>,
    pub generation_functions: HashMap<&'static str, FunctionInfo>,
    pub vector_functions: HashMap<&'static str, FunctionInfo>,
    pub differential_privacy_functions: HashMap<&'static str, FunctionInfo>,
    pub data_metric_functions: HashMap<&'static str, FunctionInfo>,
    pub data_quality_functions: HashMap<&'static str, FunctionInfo>,
    pub metadata_functions: HashMap<&'static str, FunctionInfo>,
    pub account_functions: HashMap<&'static str, FunctionInfo>,
    pub iceberg_functions: HashMap<&'static str, FunctionInfo>,
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
        Self {
            // Use generated const arrays - map category names to struct field names
            numeric_functions: build_hashmap_from_array(&NUMERIC_FUNCTIONS),
            string_binary_functions: build_hashmap_from_array(&STRING_BINARY_FUNCTIONS),
            date_time_functions: build_hashmap_from_array(&DATETIME_FUNCTIONS),
            semi_structured_functions: build_hashmap_from_array(&SEMISTRUCTURED_FUNCTIONS),
            aggregate_functions: build_hashmap_from_array(&AGGREGATE_FUNCTIONS),
            window_functions: build_hashmap_from_array(&WINDOW_FUNCTIONS),
            conditional_functions: build_hashmap_from_array(&CONDITIONAL_FUNCTIONS),
            conversion_functions: build_hashmap_from_array(&CONVERSION_FUNCTIONS),
            context_functions: build_hashmap_from_array(&CONTEXT_FUNCTIONS),
            system_functions: build_hashmap_from_array(&SYSTEM_FUNCTIONS),
            geospatial_functions: build_hashmap_from_array(&GEOSPATIAL_FUNCTIONS),
            table_functions: build_hashmap_from_array(&TABLE_FUNCTIONS),
            information_schema_functions: build_hashmap_from_array(&INFORMATION_SCHEMA_FUNCTIONS),
            bitwise_functions: build_hashmap_from_array(&BITWISE_FUNCTIONS),
            hash_functions: build_hashmap_from_array(&HASH_FUNCTIONS),
            encryption_functions: build_hashmap_from_array(&ENCRYPTION_FUNCTIONS),
            file_functions: build_hashmap_from_array(&FILE_FUNCTIONS),
            notification_functions: build_hashmap_from_array(&NOTIFICATION_FUNCTIONS),
            generation_functions: build_hashmap_from_array(&GENERATION_FUNCTIONS),
            vector_functions: build_hashmap_from_array(&VECTOR_FUNCTIONS),
            differential_privacy_functions: build_hashmap_from_array(&DIFFERENTIAL_PRIVACY_FUNCTIONS),
            data_metric_functions: build_hashmap_from_array(&DATA_METRIC_FUNCTIONS),
            data_quality_functions: build_hashmap_from_array(&DATA_QUALITY_FUNCTIONS),
            metadata_functions: build_hashmap_from_array(&METADATA_FUNCTIONS),
            account_functions: build_hashmap_from_array(&ACCOUNT_FUNCTIONS),
            iceberg_functions: build_hashmap_from_array(&ICEBERG_FUNCTIONS),
        }
    }

    /// Check if a function exists in any category
    pub fn check_function_exists(&self, function_name: &str) -> bool {
        self.numeric_functions.contains_key(function_name) ||
        self.string_binary_functions.contains_key(function_name) ||
        self.date_time_functions.contains_key(function_name) ||
        self.semi_structured_functions.contains_key(function_name) ||
        self.aggregate_functions.contains_key(function_name) ||
        self.window_functions.contains_key(function_name) ||
        self.conditional_functions.contains_key(function_name) ||
        self.conversion_functions.contains_key(function_name) ||
        self.context_functions.contains_key(function_name) ||
        self.system_functions.contains_key(function_name) ||
        self.geospatial_functions.contains_key(function_name) ||
        self.table_functions.contains_key(function_name) ||
        self.information_schema_functions.contains_key(function_name) ||
        self.bitwise_functions.contains_key(function_name) ||
        self.hash_functions.contains_key(function_name) ||
        self.encryption_functions.contains_key(function_name) ||
        self.file_functions.contains_key(function_name) ||
        self.notification_functions.contains_key(function_name) ||
        self.generation_functions.contains_key(function_name) ||
        self.vector_functions.contains_key(function_name) ||
        self.differential_privacy_functions.contains_key(function_name) ||
        self.data_metric_functions.contains_key(function_name) ||
        self.data_quality_functions.contains_key(function_name) ||
        self.metadata_functions.contains_key(function_name) ||
        self.account_functions.contains_key(function_name) ||
        self.iceberg_functions.contains_key(function_name)
    }

    /// Check if a function is unimplemented (exists in our registry)
    pub fn is_unimplemented(&self, function_name: &str) -> bool {
        let function_name_upper = function_name.to_uppercase();
        self.check_function_exists(function_name_upper.as_str())
    }

    /// Get function count by category
    pub fn get_category_counts(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("numeric", self.numeric_functions.len()),
            ("string_binary", self.string_binary_functions.len()),
            ("datetime", self.date_time_functions.len()),
            ("semi_structured", self.semi_structured_functions.len()),
            ("aggregate", self.aggregate_functions.len()),
            ("window", self.window_functions.len()),
            ("conditional", self.conditional_functions.len()),
            ("conversion", self.conversion_functions.len()),
            ("context", self.context_functions.len()),
            ("system", self.system_functions.len()),
            ("geospatial", self.geospatial_functions.len()),
            ("table", self.table_functions.len()),
            ("information_schema", self.information_schema_functions.len()),
            ("bitwise", self.bitwise_functions.len()),
            ("hash", self.hash_functions.len()),
            ("encryption", self.encryption_functions.len()),
            ("file", self.file_functions.len()),
            ("notification", self.notification_functions.len()),
            ("generation", self.generation_functions.len()),
            ("vector", self.vector_functions.len()),
            ("differential_privacy", self.differential_privacy_functions.len()),
            ("data_metric", self.data_metric_functions.len()),
            ("data_quality", self.data_quality_functions.len()),
            ("metadata", self.metadata_functions.len()),
            ("account", self.account_functions.len()),
            ("iceberg", self.iceberg_functions.len()),
        ]
    }

    /// Get total function count
    pub fn total_function_count(&self) -> usize {
        self.get_category_counts().iter().map(|(_, count)| count).sum()
    }
}

/// Create a lazy static instance of the functions registry
pub fn get_snowflake_functions() -> SnowflakeFunctions {
    SnowflakeFunctions::new()
}