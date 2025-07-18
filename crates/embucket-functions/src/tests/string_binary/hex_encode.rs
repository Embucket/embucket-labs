use crate::test_query;

// Basic functionality tests
test_query!(
    hex_encode_basic_string,
    "SELECT HEX_ENCODE('SNOW') AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_basic_string_lowercase,
    "SELECT HEX_ENCODE('SNOW', 0) AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_basic_string_uppercase,
    "SELECT HEX_ENCODE('SNOW', 1) AS hex_value",
    snapshot_path = "hex_encode"
);

// Empty and null tests
test_query!(
    hex_encode_empty_string,
    "SELECT HEX_ENCODE('') AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_null_input,
    "SELECT HEX_ENCODE(NULL) AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_null_case,
    "SELECT HEX_ENCODE('test', NULL) AS hex_value",
    snapshot_path = "hex_encode"
);

// Special characters and Unicode
test_query!(
    hex_encode_special_chars,
    "SELECT HEX_ENCODE('Hello, World!') AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_unicode,
    "SELECT HEX_ENCODE('Hello, 世界!') AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_control_chars,
    "SELECT
        HEX_ENCODE('\n') AS newline,
        HEX_ENCODE('\t') AS tab,
        HEX_ENCODE('\r') AS carriage_return",
    snapshot_path = "hex_encode"
);

// Binary data tests
test_query!(
    hex_encode_binary_literal,
    "SELECT HEX_ENCODE(X'DEADBEEF') AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_binary_literal_lowercase,
    "SELECT HEX_ENCODE(X'DEADBEEF', 0) AS hex_value",
    snapshot_path = "hex_encode"
);

// Numbers and digits
test_query!(
    hex_encode_numbers,
    "SELECT
        HEX_ENCODE('0') AS zero,
        HEX_ENCODE('123') AS numbers,
        HEX_ENCODE('9876543210') AS long_number",
    snapshot_path = "hex_encode"
);

// Case sensitivity tests
test_query!(
    hex_encode_case_comparison,
    "SELECT
        HEX_ENCODE('ABC', 1) AS uppercase,
        HEX_ENCODE('ABC', 0) AS lowercase,
        HEX_ENCODE('abc', 1) AS input_lower_output_upper,
        HEX_ENCODE('abc', 0) AS input_lower_output_lower",
    snapshot_path = "hex_encode"
);

// Table data tests
test_query!(
    hex_encode_table_data,
    "WITH test_data AS (
        SELECT 'Hello' AS text_col, 1 AS case_col
        UNION ALL
        SELECT 'World', 0
        UNION ALL
        SELECT 'Test', 1
        UNION ALL
        SELECT '', 0
        UNION ALL
        SELECT NULL, 1
    )
    SELECT
        text_col,
        case_col,
        HEX_ENCODE(text_col) AS hex_default,
        HEX_ENCODE(text_col, case_col) AS hex_with_case
    FROM test_data
    ORDER BY text_col NULLS LAST",
    snapshot_path = "hex_encode"
);

// Edge cases
test_query!(
    hex_encode_single_char,
    "SELECT
        HEX_ENCODE('A') AS single_char,
        HEX_ENCODE(' ') AS space,
        HEX_ENCODE('!') AS exclamation",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_long_string,
    "SELECT HEX_ENCODE('The quick brown fox jumps over the lazy dog') AS hex_value",
    snapshot_path = "hex_encode"
);

// Mixed data types (if supported)
test_query!(
    hex_encode_mixed_types,
    "SELECT
        HEX_ENCODE('string') AS from_string,
        HEX_ENCODE(X'48656C6C6F') AS from_binary",
    snapshot_path = "hex_encode"
);

// Comparison with expected values
test_query!(
    hex_encode_known_values,
    "SELECT
        HEX_ENCODE('A') AS should_be_41,
        HEX_ENCODE('Hello') AS should_be_48656C6C6F,
        HEX_ENCODE('0') AS should_be_30,
        HEX_ENCODE('!') AS should_be_21",
    snapshot_path = "hex_encode"
);

// Array operations with different case values
test_query!(
    hex_encode_array_operations,
    "WITH test_array AS (
        SELECT column1 AS text_val, column2 AS case_val FROM VALUES
        ('Hello', 1),
        ('World', 0),
        ('Test', 1),
        ('', 0),
        (NULL, 1)
    )
    SELECT
        text_val,
        case_val,
        HEX_ENCODE(text_val, case_val) AS hex_encoded
    FROM test_array",
    snapshot_path = "hex_encode"
);

// Error cases (these should fail gracefully)
test_query!(
    hex_encode_invalid_case_high,
    "SELECT HEX_ENCODE('test', 2) AS hex_value",
    snapshot_path = "hex_encode"
);

test_query!(
    hex_encode_invalid_case_negative,
    "SELECT HEX_ENCODE('test', -1) AS hex_value",
    snapshot_path = "hex_encode"
);

// Test with different integer types for case parameter
test_query!(
    hex_encode_different_int_types,
    "SELECT
        HEX_ENCODE('test', CAST(0 AS SMALLINT)) AS with_smallint,
        HEX_ENCODE('test', CAST(1 AS INTEGER)) AS with_integer,
        HEX_ENCODE('test', CAST(0 AS BIGINT)) AS with_bigint",
    snapshot_path = "hex_encode"
);

// Test with very long strings
test_query!(
    hex_encode_very_long_string,
    "SELECT HEX_ENCODE(REPEAT('A', 100)) AS long_hex",
    snapshot_path = "hex_encode"
);

// Test with all ASCII printable characters
test_query!(
    hex_encode_ascii_range,
    "SELECT
        HEX_ENCODE('!\"#$%&''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~') AS all_printable",
    snapshot_path = "hex_encode"
);

// Test case consistency
test_query!(
    hex_encode_case_consistency,
    "SELECT
        HEX_ENCODE('AbCdEf', 1) AS mixed_input_upper_case,
        HEX_ENCODE('AbCdEf', 0) AS mixed_input_lower_case,
        UPPER(HEX_ENCODE('AbCdEf', 0)) = HEX_ENCODE('AbCdEf', 1) AS case_conversion_works",
    snapshot_path = "hex_encode"
);
