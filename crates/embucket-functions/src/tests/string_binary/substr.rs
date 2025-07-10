use crate::tests::macros::test_query;

test_query!(
    substr_basic_positive,
    "SELECT substr('mystring', 3, 2) as result",
    snapshot_path = "substr/basic_positive"
);

test_query!(
    substr_basic_positive_no_length,
    "SELECT substr('mystring', 3) as result",
    snapshot_path = "substr/basic_positive_no_length"
);

test_query!(
    substr_negative_index_snowflake,
    "SELECT substr('mystring', -1, 3) as result",
    snapshot_path = "substr/negative_index_snowflake"
);

test_query!(
    substr_negative_index_multiple,
    "SELECT substr('mystring', -3, 2) as result",
    snapshot_path = "substr/negative_index_multiple"
);

test_query!(
    substr_negative_index_no_length,
    "SELECT substr('mystring', -3) as result",
    snapshot_path = "substr/negative_index_no_length"
);

test_query!(
    substr_zero_position,
    "SELECT substr('mystring', 0, 3) as result",
    snapshot_path = "substr/zero_position"
);

test_query!(
    substr_out_of_bounds_positive,
    "SELECT substr('mystring', 20, 3) as result",
    snapshot_path = "substr/out_of_bounds_positive"
);

test_query!(
    substr_out_of_bounds_negative,
    "SELECT substr('mystring', -20, 3) as result",
    snapshot_path = "substr/out_of_bounds_negative"
);

test_query!(
    substr_empty_string,
    "SELECT substr('', 1, 3) as result",
    snapshot_path = "substr/empty_string"
);

test_query!(
    substr_empty_string_negative,
    "SELECT substr('', -1, 3) as result",
    snapshot_path = "substr/empty_string_negative"
);

test_query!(
    substr_null_string,
    "SELECT substr(NULL, 1, 3) as result",
    snapshot_path = "substr/null_string"
);

test_query!(
    substr_null_start,
    "SELECT substr('mystring', NULL, 3) as result",
    snapshot_path = "substr/null_start"
);

test_query!(
    substr_null_length,
    "SELECT substr('mystring', 1, NULL) as result",
    snapshot_path = "substr/null_length"
);

test_query!(
    substr_zero_length,
    "SELECT substr('mystring', 3, 0) as result",
    snapshot_path = "substr/zero_length"
);

test_query!(
    substr_unicode_chars,
    "SELECT substr('héllo🌏world', 3, 4) as result",
    snapshot_path = "substr/unicode_chars"
);

test_query!(
    substr_unicode_negative,
    "SELECT substr('héllo🌏world', -3, 2) as result",
    snapshot_path = "substr/unicode_negative"
);

test_query!(
    substr_long_string,
    "SELECT substr('this is a very long string for testing purposes', 10, 8) as result",
    snapshot_path = "substr/long_string"
);

test_query!(
    substr_long_string_negative,
    "SELECT substr('this is a very long string for testing purposes', -8, 5) as result",
    snapshot_path = "substr/long_string_negative"
);

test_query!(
    substring_alias,
    "SELECT substring('mystring', 3, 2) as result",
    snapshot_path = "substr/substring_alias"
);

test_query!(
    substring_alias_negative,
    "SELECT substring('mystring', -2, 2) as result",
    snapshot_path = "substr/substring_alias_negative"
);

test_query!(
    substr_edge_case_one_char,
    "SELECT substr('a', 1, 1) as result",
    snapshot_path = "substr/edge_case_one_char"
);

test_query!(
    substr_edge_case_one_char_negative,
    "SELECT substr('a', -1, 1) as result",
    snapshot_path = "substr/edge_case_one_char_negative"
);

test_query!(
    substr_multiple_rows,
    "SELECT substr(col, 2, 3) as result FROM (VALUES ('hello'), ('world'), ('test')) AS t(col)",
    snapshot_path = "substr/multiple_rows"
);

test_query!(
    substr_multiple_rows_negative,
    "SELECT substr(col, -2, 2) as result FROM (VALUES ('hello'), ('world'), ('test')) AS t(col)",
    snapshot_path = "substr/multiple_rows_negative"
);

test_query!(
    substr_comparison_with_datafusion_positive,
    "SELECT 
        'mystring' as original,
        substr('mystring', 3, 2) as snowflake_result,
        'st' as expected_result",
    snapshot_path = "substr/comparison_positive"
);

test_query!(
    substr_comparison_with_datafusion_negative,
    "SELECT 
        'mystring' as original,
        substr('mystring', -1, 3) as snowflake_result,
        'g' as expected_snowflake,
        'mys' as would_be_postgresql",
    snapshot_path = "substr/comparison_negative"
);

test_query!(
    substr_negative_length_error,
    "SELECT substr('mystring', 1, -1) as result",
    snapshot_path = "substr/negative_length_error"
);

test_query!(
    substr_large_length,
    "SELECT substr('mystring', 3, 100) as result",
    snapshot_path = "substr/large_length"
);

test_query!(
    substr_large_negative_start,
    "SELECT substr('mystring', -100, 5) as result",
    snapshot_path = "substr/large_negative_start"
);
