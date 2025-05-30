use crate::test_query;

// ACCOUNT_FUNCTIONS
test_query!(
    unimplemented_account_function,
    "SELECT CUMULATIVE_PRIVACY_LOSSES()"
);

// AGGREGATE_FUNCTIONS  
test_query!(
    unimplemented_aggregate_function,
    "SELECT ANY_VALUE(column1) FROM employee_table"
);

// BITWISE_FUNCTIONS
test_query!(
    unimplemented_bitwise_function,
    "SELECT BITAND(15, 7)"
);

// CONDITIONAL_FUNCTIONS
test_query!(
    unimplemented_conditional_function,
    "SELECT BOOLAND(true, false)"
);

// CONTEXT_FUNCTIONS
test_query!(
    unimplemented_context_function,
    "SELECT CURRENT_AVAILABLE_ROLES()"
);

// CONVERSION_FUNCTIONS
test_query!(
    unimplemented_conversion_function,
    "SELECT TO_CHAR(123)"
);

// DATA_METRIC_FUNCTIONS
test_query!(
    unimplemented_data_metric_function,
    "SELECT DM_MEAN_SQUARED_DIFFERENCE(col1, col2) FROM employee_table"
);

// DATA_QUALITY_FUNCTIONS
test_query!(
    unimplemented_data_quality_function,
    "SELECT DQ_RULE('rule1', 'expression')"
);

// DATETIME_FUNCTIONS
test_query!(
    unimplemented_datetime_function,
    "SELECT ADD_MONTHS('2023-01-01', 3)"
);

// DIFFERENTIAL_PRIVACY_FUNCTIONS
test_query!(
    unimplemented_differential_privacy_function,
    "SELECT DP_COUNT(*) FROM employee_table"
);

// ENCRYPTION_FUNCTIONS
test_query!(
    unimplemented_encryption_function,
    "SELECT DECRYPT('test', 'key')"
);

// FILE_FUNCTIONS
test_query!(
    unimplemented_file_function,
    "SELECT BUILD_SCOPED_FILE_URL('@my_stage', 'file.txt')"
);

// GENERATION_FUNCTIONS
test_query!(
    unimplemented_generation_function,
    "SELECT AUTOINCREMENT_NEXTVAL('seq1')"
);

// GEOSPATIAL_FUNCTIONS
test_query!(
    unimplemented_geospatial_function,
    "SELECT ST_AREA(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"
);

// HASH_FUNCTIONS
test_query!(
    unimplemented_hash_function,
    "SELECT HASH(column1) FROM employee_table"
);

// ICEBERG_FUNCTIONS
test_query!(
    unimplemented_iceberg_function,
    "SELECT ICEBERG_TABLE_FILES('table1')"
);

// INFORMATION_SCHEMA_FUNCTIONS
test_query!(
    unimplemented_information_schema_function,
    "SELECT APPLICABLE_ROLES()"
);

// METADATA_FUNCTIONS
test_query!(
    unimplemented_metadata_function,
    "SELECT GET_DDL('table', 'employee_table')"
);

// NOTIFICATION_FUNCTIONS
test_query!(
    unimplemented_notification_function,
    "SELECT SYSTEM$SEND_EMAIL('to@email.com', 'subject', 'body')"
);

// NUMERIC_FUNCTIONS
test_query!(
    unimplemented_numeric_function,
    "SELECT ACOS(0.5)"
);

// SEMISTRUCTURED_FUNCTIONS
test_query!(
    unimplemented_semistructured_function,
    "SELECT ARRAYS_OVERLAP(ARRAY[1, 2], ARRAY[2, 3])"
);

// STRING_BINARY_FUNCTIONS
test_query!(
    unimplemented_string_binary_function,
    "SELECT BASE32_ENCODE('hello')"
);

// SYSTEM_FUNCTIONS
test_query!(
    unimplemented_system_function,
    "SELECT EXTRACT_SEMANTIC_CATEGORIES('table1')"
);

// TABLE_FUNCTIONS
test_query!(
    unimplemented_table_function,
    "SELECT * FROM FLATTEN(ARRAY[1, 2, 3])"
);

// VECTOR_FUNCTIONS
test_query!(
    unimplemented_vector_function,
    "SELECT VECTOR_COSINE_SIMILARITY(ARRAY[1, 2], ARRAY[3, 4])"
);

// WINDOW_FUNCTIONS
test_query!(
    unimplemented_window_function,
    "SELECT CUME_DIST() OVER (ORDER BY column1) FROM employee_table"
);
