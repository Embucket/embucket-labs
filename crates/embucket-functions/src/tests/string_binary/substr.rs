use crate::test_query;

test_query!(
    substr_basic_positive,
    "SELECT substr('mystring', 3, 2) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_basic_positive_no_length,
    "SELECT substr('mystring', 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_negative_index_multiple,
    "SELECT substr('mystring', -3, 2) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_negative_index_no_length,
    "SELECT substr('mystring', -3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_zero_position,
    "SELECT substr('mystring', 0, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_out_of_bounds_positive,
    "SELECT substr('mystring', 20, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_out_of_bounds_negative,
    "SELECT substr('mystring', -20, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_empty_string,
    "SELECT substr('', 1, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_empty_string_negative,
    "SELECT substr('', -1, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_null_string,
    "SELECT substr(NULL, 1, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_null_start,
    "SELECT substr('mystring', NULL, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_null_length,
    "SELECT substr('mystring', 1, NULL) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_zero_length,
    "SELECT substr('mystring', 3, 0) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_unicode_chars,
    "SELECT substr('héllo🌏world', 3, 4) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_unicode_negative,
    "SELECT substr('héllo🌏world', -3, 2) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_long_string,
    "SELECT substr('this is a very long string for testing purposes', 10, 8) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_long_string_negative,
    "SELECT substr('this is a very long string for testing purposes', -8, 5) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_edge_case_one_char,
    "SELECT substr('a', 1, 1) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_edge_case_one_char_negative,
    "SELECT substr('a', -1, 1) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_multiple_rows,
    "SELECT substr(col, 2, 3) as result FROM (VALUES ('hello'), ('world'), ('test')) AS t(col)",
    snapshot_path = "substr"
);

test_query!(
    substr_multiple_rows_negative,
    "SELECT substr(col, -2, 2) as result FROM (VALUES ('hello'), ('world'), ('test')) AS t(col)",
    snapshot_path = "substr"
);

test_query!(
    substr_negative_length_error,
    "SELECT substr('mystring', 1, -1) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_large_length,
    "SELECT substr('mystring', 3, 100) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_large_negative_start,
    "SELECT substr('mystring', -100, 5) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_integer_positive,
    "SELECT substr(12345, 2, 3) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_integer_negative,
    "SELECT substr(-567, -2, 2) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_decimal_positive,
    "SELECT substr(123.456, 3, 4) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_decimal_negative_index,
    "SELECT substr(1.23, -2, 2) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_basic,
    "SELECT substr(hex_encode('hello world'), 2, 5) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_negative,
    "SELECT substr(hex_encode('hello world'), -5, 4) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_emoji,
    "SELECT substr(hex_encode('test🚀data'), 5, 4) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_emoji_negative,
    "SELECT substr(hex_encode('test🚀data'), -8, 4) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_mixed,
    "SELECT substr(hex_encode('café☕test'), 4, 5) as result",
    snapshot_path = "substr"
);

test_query!(
    substr_hex_encode_empty,
    "SELECT substr(hex_encode(''), 1, 3) as result",
    snapshot_path = "substr"
);
