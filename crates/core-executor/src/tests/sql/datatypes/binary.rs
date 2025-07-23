use crate::test_query;

test_query!(
    binary_basic_table_creation,
    "SELECT * FROM binary_test_table ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_test_table (
            id INT, 
            data BINARY(16),
            variable_data VARBINARY(255)
        )",
        "INSERT INTO binary_test_table VALUES 
            (1, TO_BINARY('SNOW', 'UTF-8'), TO_BINARY('Hello World', 'UTF-8')),
            (2, TO_BINARY('534E4F57', 'HEX'), TO_BINARY('48656C6C6F', 'HEX')),
            (3, TO_BINARY('U05PVw==', 'BASE64'), TO_BINARY('SGVsbG8gV29ybGQ=', 'BASE64'))"
    ],
    snapshot_path = "binary"
);

test_query!(
    binary_to_binary_formats,
    "SELECT 
        TO_BINARY('EMBUCKET', 'UTF-8') AS utf8_binary,
        TO_BINARY('454D4255434B4554', 'HEX') AS hex_binary,
        TO_BINARY('RU1CVUNLRVQ=', 'BASE64') AS base64_binary",
    snapshot_path = "binary"
);

test_query!(
    binary_null_handling,
    "SELECT * FROM binary_null_test ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_null_test (
            id INT,
            binary_col BINARY,
            varbinary_col VARBINARY
        )",
        "INSERT INTO binary_null_test VALUES 
            (1, TO_BINARY('test', 'UTF-8'), TO_BINARY('value', 'UTF-8')),
            (2, NULL, TO_BINARY('only_varbinary', 'UTF-8')),
            (3, TO_BINARY('only_binary', 'UTF-8'), NULL),
            (4, NULL, NULL)"
    ],
    snapshot_path = "binary"
);

test_query!(
    binary_operations,
    "SELECT 
        LENGTH(binary_col) AS binary_length,
        LENGTH(varbinary_col) AS varbinary_length,
        binary_col = TO_BINARY('test', 'UTF-8') AS binary_equals,
        varbinary_col IS NULL AS varbinary_is_null
    FROM binary_null_test 
    ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_null_test (
            id INT,
            binary_col BINARY,
            varbinary_col VARBINARY
        )",
        "INSERT INTO binary_null_test VALUES 
            (1, TO_BINARY('test', 'UTF-8'), TO_BINARY('value', 'UTF-8')),
            (2, NULL, TO_BINARY('only_varbinary', 'UTF-8')),
            (3, TO_BINARY('only_binary', 'UTF-8'), NULL),
            (4, NULL, NULL)"
    ],
    snapshot_path = "binary"
);

// Test TRY_TO_BINARY for error handling
test_query!(
    binary_try_to_binary,
    "SELECT 
        TRY_TO_BINARY('ValidHex', 'HEX') AS valid_hex,
        TRY_TO_BINARY('ValidBase64==', 'BASE64') AS valid_base64,
        TRY_TO_BINARY('InvalidHex', 'HEX') AS invalid_hex,
        TRY_TO_BINARY('Invalid==Base64', 'BASE64') AS invalid_base64,
        TRY_TO_BINARY('test', 'INVALID_FORMAT') AS invalid_format",
    snapshot_path = "binary"
);

// Test binary data with mixed content types
test_query!(
    binary_mixed_content,
    "SELECT * FROM binary_mixed_table ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_mixed_table (
            id INT,
            description VARCHAR(50),
            utf8_data BINARY,
            hex_data VARBINARY,
            base64_data BINARY
        )",
        "INSERT INTO binary_mixed_table VALUES 
            (1, 'Text as binary', TO_BINARY('Hello, World!', 'UTF-8'), TO_BINARY('48656C6C6F2C20576F726C6421', 'HEX'), TO_BINARY('SGVsbG8sIFdvcmxkIQ==', 'BASE64')),
            (2, 'Numbers', TO_BINARY('12345', 'UTF-8'), TO_BINARY('3132333435', 'HEX'), TO_BINARY('MTIzNDU=', 'BASE64')),
            (3, 'Special chars', TO_BINARY('!@#$%', 'UTF-8'), TO_BINARY('2402324256', 'HEX'), TO_BINARY('IUAjJCU=', 'BASE64'))"
    ],
    snapshot_path = "binary"
);

test_query!(
    binary_aggregations,
    "SELECT 
        COUNT(*) AS total_rows,
        COUNT(binary_col) AS non_null_binary,
        COUNT(varbinary_col) AS non_null_varbinary,
        MIN(LENGTH(binary_col)) AS min_binary_length,
        MAX(LENGTH(binary_col)) AS max_binary_length,
        MIN(LENGTH(varbinary_col)) AS min_varbinary_length,
        MAX(LENGTH(varbinary_col)) AS max_varbinary_length
    FROM binary_null_test",
    setup_queries = [
        "CREATE TABLE binary_null_test (
            id INT,
            binary_col BINARY,
            varbinary_col VARBINARY
        )",
        "INSERT INTO binary_null_test VALUES 
            (1, TO_BINARY('test', 'UTF-8'), TO_BINARY('value', 'UTF-8')),
            (2, NULL, TO_BINARY('only_varbinary', 'UTF-8')),
            (3, TO_BINARY('only_binary', 'UTF-8'), NULL),
            (4, NULL, NULL)"
    ],
    snapshot_path = "binary"
);

test_query!(
    binary_cte_subquery,
    "WITH binary_cte AS (
        SELECT 
            id,
            TO_BINARY(CONCAT('prefix_', CAST(id AS STRING)), 'UTF-8') AS computed_binary,
            TO_BINARY('CONSTANT', 'UTF-8') AS constant_binary
        FROM (VALUES (1), (2), (3)) AS t(id)
    )
    SELECT 
        id,
        computed_binary,
        constant_binary,
        LENGTH(computed_binary) AS computed_length,
        LENGTH(constant_binary) AS constant_length
    FROM binary_cte
    ORDER BY id",
    snapshot_path = "binary"
);

test_query!(
    binary_case_insensitive_formats,
    "SELECT 
        TO_BINARY('EMBUCKET', 'utf-8') AS utf8_lower,
        TO_BINARY('454D4255434B4554', 'hex') AS hex_lower,
        TO_BINARY('RU1CVUNLRVQ=', 'base64') AS base64_lower,
        TO_BINARY('EMBUCKET', 'UTF-8') AS utf8_upper,
        TO_BINARY('454D4255434B4554', 'HEX') AS hex_upper,
        TO_BINARY('RU1CVUNLRVQ=', 'BASE64') AS base64_upper",
    snapshot_path = "binary"
);

test_query!(
    binary_create_table_as,
    "SELECT * FROM binary_derived_table ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_source AS
        SELECT 
            ROW_NUMBER() OVER () AS id,
            TO_BINARY('test_' || CAST(ROW_NUMBER() OVER () AS STRING), 'UTF-8') AS binary_data
        FROM (VALUES (1), (2), (3)) AS t(x)",
        "CREATE TABLE binary_derived_table AS
        SELECT 
            id,
            binary_data,
            TO_BINARY('derived_' || CAST(id AS STRING), 'UTF-8') AS derived_binary
        FROM binary_source"
    ],
    snapshot_path = "binary"
);

test_query!(
    binary_size_flexibility,
    "SELECT * FROM binary_size_test ORDER BY id",
    setup_queries = [
        "CREATE TABLE binary_size_test (
            id INT,
            binary_no_size BINARY,
            binary_with_size BINARY(32),
            varbinary_no_size VARBINARY,
            varbinary_with_size VARBINARY(64)
        )",
        "INSERT INTO binary_size_test VALUES 
            (1, TO_BINARY('small', 'UTF-8'), TO_BINARY('sized_binary', 'UTF-8'), TO_BINARY('variable', 'UTF-8'), TO_BINARY('sized_variable', 'UTF-8')),
            (2, TO_BINARY('456789AB', 'HEX'), TO_BINARY('DEADBEEF', 'HEX'), TO_BINARY('CAFEBABE', 'HEX'), TO_BINARY('FEEDFACE', 'HEX'))"
    ],
    snapshot_path = "binary"
);
