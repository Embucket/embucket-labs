use crate::test_query;

// Basic functionality tests
test_query!(
    basic_string,
    "SELECT LOWER('HELLO WORLD') AS result",
    snapshot_path = "lower"
);

test_query!(
    mixed_case,
    "SELECT LOWER('Hello World') AS result",
    snapshot_path = "lower"
);

test_query!(
    already_lowercase,
    "SELECT LOWER('hello world') AS result",
    snapshot_path = "lower"
);

test_query!(
    empty_string,
    "SELECT LOWER('') AS result",
    snapshot_path = "lower"
);

test_query!(
    null_value,
    "SELECT LOWER(NULL) AS result",
    snapshot_path = "lower"
);

// Unicode character handling
test_query!(
    unicode_characters,
    "SELECT 
        LOWER('CAFÉ') AS accented, 
        LOWER('Café') AS mixed_accented,
        LOWER('你好世界') AS chinese,
        LOWER('ΕΛΛΗΝΙΚΆ') AS greek,
        LOWER('Ελληνικά') AS mixed_greek",
    snapshot_path = "lower"
);

// Numeric and non-string types
test_query!(
    non_string_types,
    "SELECT 
        LOWER(123) AS number,
        LOWER(123.45) AS float,
        LOWER(CAST(123.45 AS DECIMAL(10,2))) AS decimal",
    snapshot_path = "lower"
);
