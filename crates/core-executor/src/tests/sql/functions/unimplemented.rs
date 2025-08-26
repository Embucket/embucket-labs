use crate::test_query;

// ACCOUNT_FUNCTIONS
test_query!(
    unimplemented_account_function,
    "SELECT CUMULATIVE_PRIVACY_LOSSES()",
    snapshot_path = "unimplemented"
);

// AGGREGATE_FUNCTIONS
test_query!(
    unimplemented_aggregate_function,
    "SELECT APPROXIMATE_SIMILARITY(column1) FROM employee_table",
    snapshot_path = "unimplemented"
);

// BITWISE_FUNCTIONS
test_query!(
    unimplemented_bitwise_function,
    "SELECT BITSHIFTLEFT(15, 7)",
    snapshot_path = "unimplemented"
);

// CONDITIONAL_FUNCTIONS
test_query!(
    unimplemented_conditional_function,
    "SELECT GREATEST_IGNORE_NULLS(true, false)",
    snapshot_path = "unimplemented"
);

// CONTEXT_FUNCTIONS
test_query!(
    unimplemented_context_function,
    "SELECT CURRENT_AVAILABLE_ROLES()",
    snapshot_path = "unimplemented"
);

// DATA_METRIC_FUNCTIONS
// TODO: Pass real functions names
test_query!(
    unimplemented_data_metric_function,
    "SELECT FRESHNESS(la)",
    snapshot_path = "unimplemented"
);

// DATA_QUALITY_FUNCTIONS
test_query!(
    unimplemented_data_quality_function,
    "SELECT DATA_QUALITY_MONITORING_RESULTS('rule1', 'expression')",
    snapshot_path = "unimplemented"
);

// DIFFERENTIAL_PRIVACY_FUNCTIONS
test_query!(
    unimplemented_differential_privacy_function,
    "SELECT DP_INTERVAL_HIGH(*)",
    snapshot_path = "unimplemented"
);

// ENCRYPTION_FUNCTIONS
test_query!(
    unimplemented_encryption_function,
    "SELECT DECRYPT('test', 'key')",
    snapshot_path = "unimplemented"
);

// FILE_FUNCTIONS
test_query!(
    unimplemented_file_function,
    "SELECT BUILD_SCOPED_FILE_URL('@my_stage', 'file.txt')",
    snapshot_path = "unimplemented"
);

// GENERATION_FUNCTIONS
test_query!(
    unimplemented_generation_function,
    "SELECT NORMAL('seq1')",
    snapshot_path = "unimplemented"
);

// GEOSPATIAL_FUNCTIONS
test_query!(
    unimplemented_geospatial_function,
    "SELECT H3_CELL_TO_BOUNDARY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')",
    snapshot_path = "unimplemented"
);

// HASH_FUNCTIONS
test_query!(
    unimplemented_hash_function,
    "SELECT HASH(column1) FROM employee_table",
    snapshot_path = "unimplemented"
);

// ICEBERG_FUNCTIONS
test_query!(
    unimplemented_iceberg_function,
    "SELECT ICEBERG_TABLE_FILES('table1')",
    snapshot_path = "unimplemented"
);

// INFORMATION_SCHEMA_FUNCTIONS
test_query!(
    unimplemented_information_schema_function,
    "SELECT WAREHOUSE_METERING_HISTORY()",
    snapshot_path = "unimplemented"
);

// METADATA_FUNCTIONS
test_query!(
    unimplemented_metadata_function,
    "SELECT GET_DDL('table', 'employee_table')",
    snapshot_path = "unimplemented"
);

// NOTIFICATION_FUNCTIONS
test_query!(
    unimplemented_notification_function,
    "SELECT APPLICATION_JSON()",
    snapshot_path = "unimplemented"
);

// SEMISTRUCTURED_FUNCTIONS
test_query!(
    unimplemented_semistructured_function,
    "SELECT CHECK_JSON('{\"key\": \"value\"}')",
    snapshot_path = "unimplemented"
);

// STRING_BINARY_FUNCTIONS
test_query!(
    unimplemented_string_binary_function,
    "SELECT BASE64_DECODE_BINARY('hello')",
    snapshot_path = "unimplemented"
);

// SYSTEM_FUNCTIONS
test_query!(
    unimplemented_system_function,
    "SELECT SYSTEM$GET_CMK_INFO()",
    snapshot_path = "unimplemented"
);

// TABLE_FUNCTIONS
test_query!(
    unimplemented_table_function,
    "SELECT GENERATOR(10)",
    snapshot_path = "unimplemented"
);

// VECTOR_FUNCTIONS
test_query!(
    unimplemented_vector_function,
    "SELECT VECTOR_COSINE_SIMILARITY(ARRAY[1, 2], ARRAY[3, 4])",
    snapshot_path = "unimplemented"
);
