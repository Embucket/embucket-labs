use crate::test_query;

test_query!(
    basic_floats,
    "SELECT 
        TO_VARCHAR(123.45::FLOAT) AS float32_val,
        TO_VARCHAR(-987.654321::DOUBLE) AS float64_val,
        TO_VARCHAR(0.0::FLOAT) AS zero_float,
        TO_VARCHAR(42.0::DOUBLE) AS whole_number_float",
    snapshot_path = "to_varchar"
);

test_query!(
    basic_strings,
    "SELECT 
        TO_VARCHAR('hello world') AS string_val,
        TO_VARCHAR('') AS empty_string,
        TO_VARCHAR('unicode: 你好🌍') AS unicode_string",
    snapshot_path = "to_varchar"
);

test_query!(
    basic_dates,
    "SELECT 
        TO_VARCHAR('2024-03-15'::DATE) AS date_val,
        TO_VARCHAR('2024-03-15 14:30:45'::TIMESTAMP) AS timestamp_val",
    snapshot_path = "to_varchar"
);

test_query!(
    null_values,
    "SELECT 
        TO_VARCHAR(NULL::INT) AS null_int,
        TO_VARCHAR(NULL::STRING) AS null_string,
        TO_VARCHAR(NULL::DATE) AS null_date",
    snapshot_path = "to_varchar"
);

// Basic conversions using TO_CHAR alias
test_query!(
    to_char_basic,
    "SELECT 
        TO_CHAR(123) AS int_val,
        TO_CHAR(45.67) AS float_val,
        TO_CHAR('hello') AS string_val,
        TO_CHAR('2024-03-15'::DATE) AS date_val",
    snapshot_path = "to_varchar"
);

// Numeric formatting tests based on Snowflake examples
test_query!(
    numeric_formatting_dollar,
    "SELECT 
        TO_VARCHAR(-12.391, '\">\"\\$99.0\"<\"') AS neg_currency,
        TO_VARCHAR(0, '\">\"\\$99.0\"<\"') AS zero_currency,
        TO_VARCHAR(123.456, '\">\"\\$99.0\"<\"') AS pos_currency,
        TO_VARCHAR(3987, '\">\"\\$99.0\"<\"') AS overflow_currency",
    snapshot_path = "to_varchar"
);

test_query!(
    numeric_formatting_blank_zero,
    "SELECT 
        TO_VARCHAR(-12.391, '\">\"B9,999.0\"<\"') AS neg_blank,
        TO_VARCHAR(0, '\">\"B9,999.0\"<\"') AS zero_blank,
        TO_VARCHAR(123.456, '\">\"B9,999.0\"<\"') AS pos_blank,
        TO_VARCHAR(3987, '\">\"B9,999.0\"<\"') AS large_blank",
    snapshot_path = "to_varchar"
);

test_query!(
    numeric_formatting_scientific,
    "SELECT 
        TO_VARCHAR(-12.391, '\">\"TME\"<\"') AS neg_sci,
        TO_VARCHAR(0, '\">\"TME\"<\"') AS zero_sci,
        TO_VARCHAR(123.456, '\">\"TME\"<\"') AS pos_sci,
        TO_VARCHAR(3987, '\">\"TME\"<\"') AS large_sci",
    snapshot_path = "to_varchar"
);

test_query!(
    numeric_formatting_text_minimal,
    "SELECT 
        TO_VARCHAR(-12.391, '\">\"TM9\"<\"') AS neg_tm,
        TO_VARCHAR(0, '\">\"TM9\"<\"') AS zero_tm,
        TO_VARCHAR(123.456, '\">\"TM9\"<\"') AS pos_tm,
        TO_VARCHAR(3987, '\">\"TM9\"<\"') AS large_tm",
    snapshot_path = "to_varchar"
);

test_query!(
    numeric_formatting_hex,
    "SELECT 
        TO_VARCHAR(-12, '\">\"0XXX\"<\"') AS neg_hex,
        TO_VARCHAR(0, '\">\"0XXX\"<\"') AS zero_hex,
        TO_VARCHAR(255, '\">\"0XXX\"<\"') AS pos_hex,
        TO_VARCHAR(3987, '\">\"0XXX\"<\"') AS large_hex",
    snapshot_path = "to_varchar"
);

test_query!(
    numeric_formatting_signed_hex,
    "SELECT 
        TO_VARCHAR(-12, '\">\"S0XXX\"<\"') AS neg_signed_hex,
        TO_VARCHAR(0, '\">\"S0XXX\"<\"') AS zero_signed_hex,
        TO_VARCHAR(255, '\">\"S0XXX\"<\"') AS pos_signed_hex,
        TO_VARCHAR(3987, '\">\"S0XXX\"<\"') AS large_signed_hex",
    snapshot_path = "to_varchar"
);

// Date formatting tests
test_query!(
    date_formatting_default,
    "SELECT 
        TO_VARCHAR('2024-04-03'::DATE) AS default_date,
        TO_VARCHAR('2024-04-05 01:02:03'::TIMESTAMP) AS default_timestamp",
    snapshot_path = "to_varchar"
);

test_query!(
    date_formatting_custom,
    "SELECT 
        TO_VARCHAR('2024-04-03'::DATE, 'yyyy.mm.dd') AS dot_date,
        TO_VARCHAR('2024-04-03'::DATE, 'dd/mm/yyyy') AS slash_date,
        TO_VARCHAR('2024-04-03'::DATE, 'mon dd, yyyy') AS month_date",
    snapshot_path = "to_varchar"
);

test_query!(
    timestamp_formatting_custom,
    "SELECT 
        TO_VARCHAR('2024-04-05 01:02:03'::TIMESTAMP, 'mm/dd/yyyy, hh24:mi') AS us_format,
        TO_VARCHAR('2024-04-05 01:02:03'::TIMESTAMP, 'yyyy-mm-dd hh24:mi') AS iso_format,
        TO_VARCHAR('2024-04-05 01:02:03'::TIMESTAMP, 'dd mon yyyy') AS readable_format",
    snapshot_path = "to_varchar"
);

// TO_CHAR with numeric formatting
test_query!(
    to_char_numeric_formatting,
    "SELECT 
        TO_CHAR(-12.391, '\">\"\\$99.0\"<\"') AS dollar_format,
        TO_CHAR(0, '\">\"TME\"<\"') AS scientific_format,
        TO_CHAR(255, '\">\"0XXX\"<\"') AS hex_format",
    snapshot_path = "to_varchar"
);

// TO_CHAR with date formatting
test_query!(
    to_char_date_formatting,
    "SELECT 
        TO_CHAR('2024-04-03'::DATE, 'yyyy.mm.dd') AS dot_date,
        TO_CHAR('2024-04-05 01:02:03'::TIMESTAMP, 'mm/dd/yyyy, hh24:mi') AS us_timestamp",
    snapshot_path = "to_varchar"
);

// TRY_TO_VARCHAR tests - should return NULL on errors instead of failing
test_query!(
    try_to_varchar_basic,
    "SELECT 
        TRY_TO_VARCHAR(123) AS valid_int,
        TRY_TO_VARCHAR(45.67) AS valid_float,
        TRY_TO_VARCHAR('hello') AS valid_string,
        TRY_TO_VARCHAR(NULL) AS null_input",
    snapshot_path = "to_varchar"
);

test_query!(
    try_to_varchar_formatting,
    "SELECT 
        TRY_TO_VARCHAR(123, '\">\"TM9\"<\"') AS valid_format,
        TRY_TO_VARCHAR('2024-04-03'::DATE, 'yyyy.mm.dd') AS valid_date_format",
    snapshot_path = "to_varchar"
);

// TRY_TO_CHAR tests
test_query!(
    try_to_char_basic,
    "SELECT 
        TRY_TO_CHAR(123) AS valid_int,
        TRY_TO_CHAR(45.67) AS valid_float,
        TRY_TO_CHAR('hello') AS valid_string,
        TRY_TO_CHAR(NULL) AS null_input",
    snapshot_path = "to_varchar"
);

test_query!(
    try_to_char_formatting,
    "SELECT 
        TRY_TO_CHAR(123, '\">\"TM9\"<\"') AS valid_format,
        TRY_TO_CHAR('2024-04-03'::DATE, 'yyyy.mm.dd') AS valid_date_format",
    snapshot_path = "to_varchar"
);

// Edge cases and special values
test_query!(
    special_numeric_values,
    "SELECT 
        TO_VARCHAR(0) AS zero,
        TO_VARCHAR(-0.0) AS negative_zero,
        TO_VARCHAR(1.0) AS one_float,
        TO_VARCHAR(-1.0) AS neg_one_float",
    snapshot_path = "to_varchar"
);

test_query!(
    small_numbers,
    "SELECT 
        TO_VARCHAR(0.001) AS small_decimal,
        TO_VARCHAR(0.0001) AS smaller_decimal,
        TO_VARCHAR(1e-10) AS tiny_float",
    snapshot_path = "to_varchar"
);

// Format edge cases
test_query!(
    format_without_brackets,
    "SELECT 
        TO_VARCHAR(123, 'TM9') AS tm9_simple,
        TO_VARCHAR(123, 'TME') AS tme_simple,
        TO_VARCHAR(123, '\\$99.0') AS dollar_simple",
    snapshot_path = "to_varchar"
);

// Comprehensive mixed test with all aliases
test_query!(
    all_aliases_comparison,
    "SELECT 
        TO_VARCHAR(123.45) AS to_varchar,
        TO_CHAR(123.45) AS to_char,
        TRY_TO_VARCHAR(123.45) AS try_to_varchar,
        TRY_TO_CHAR(123.45) AS try_to_char",
    snapshot_path = "to_varchar"
);

// Table-based tests with multiple values
test_query!(
    table_based_conversion,
    "WITH test_data AS (
        SELECT column1 AS orig_value FROM VALUES
        (-12.391),
        (0),
        (-1),
        (0.10),
        (0.01),
        (3987),
        (1.111)
    )
    SELECT 
        orig_value,
        TO_VARCHAR(orig_value, '\">\"\\$99.0\"<\"') AS D2_1,
        TO_VARCHAR(orig_value, '\">\"B9,999.0\"<\"') AS D4_1,
        TO_VARCHAR(orig_value, '\">\"TME\"<\"') AS TME,
        TO_VARCHAR(orig_value, '\">\"TM9\"<\"') AS TM9,
        TO_VARCHAR(orig_value, '\">\"0XXX\"<\"') AS X4,
        TO_VARCHAR(orig_value, '\">\"S0XXX\"<\"') AS SX4
    FROM test_data",
    snapshot_path = "to_varchar"
);

// Binary conversion tests (basic implementation)
test_query!(
    binary_conversion_basic,
    "SELECT 
        TO_VARCHAR(TO_BINARY('SNOW', 'UTF-8')) AS binary_to_string,
        TO_VARCHAR(TO_BINARY('world', 'UTF-8'), 'UTF-8') AS binary_with_format",
    snapshot_path = "to_varchar"
);

// Time conversion tests
test_query!(
    time_conversion,
    "SELECT 
        TO_VARCHAR('12:34:56'::TIME) AS time_basic,
        TO_VARCHAR('12:34:56'::TIME, 'hh24:mi:ss') AS time_formatted",
    snapshot_path = "to_varchar"
);
