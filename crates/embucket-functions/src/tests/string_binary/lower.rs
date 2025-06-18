use crate::test_query;

// Basic functionality tests
test_query!(
    basic_string,
    "SELECT LOWER('HELLO WORLD')",
    snapshot_path = "lower"
);

test_query!(
    mixed_case,
    "SELECT LOWER('Hello World')",
    snapshot_path = "lower"
);

test_query!(
    already_lowercase,
    "SELECT LOWER('hello world')",
    snapshot_path = "lower"
);

test_query!(empty_string, "SELECT LOWER('')", snapshot_path = "lower");

test_query!(null_value, "SELECT LOWER(NULL)", snapshot_path = "lower");

// Unicode character handling
test_query!(
    unicode_characters,
    "SELECT 
        LOWER('CAFÉ'), 
        LOWER('Café'),
        LOWER('你好世界'),
        LOWER('ΕΛΛΗΝΙΚΆ'),
        LOWER('Ελληνικά')",
    snapshot_path = "lower"
);

// Numeric and non-string types
test_query!(
    non_string_types,
    "SELECT 
        LOWER(123),
        LOWER(123.45),
        LOWER(CAST(123.46 AS DECIMAL(10,2)))",
    snapshot_path = "lower"
);
