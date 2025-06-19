use crate::test_query;

test_query!(
    to_binary_utf8,
    "SELECT TO_BINARY('SNOW', 'UTF-8') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_hex,
    "SELECT TO_BINARY('534E4F57', 'HEX') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_base64,
    "SELECT TO_BINARY('U05PVw==', 'BASE64') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_default_format,
    "SELECT TO_BINARY('SNOW') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_null,
    "SELECT TO_BINARY(NULL, 'UTF-8') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    try_to_binary_valid,
    "SELECT TRY_TO_BINARY('534E4F57', 'HEX') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    try_to_binary_invalid,
    "SELECT TRY_TO_BINARY('ZZZZ', 'HEX') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    try_to_binary_invalid_format,
    "SELECT TRY_TO_BINARY('SNOW', 'INVALID_FORMAT') AS binary_value",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_table,
    "WITH test_data AS (
        SELECT 'SNOW' AS s, '534E4F57' AS hex, 'U05PVw==' AS b64
    )
    SELECT 
        TO_BINARY(s, 'UTF-8') AS utf8_bin,
        TO_BINARY(hex, 'HEX') AS hex_bin,
        TO_BINARY(b64, 'BASE64') AS b64_bin
    FROM test_data",
    snapshot_path = "to_binary"
);

test_query!(
    to_binary_case_insensitive_format,
    "SELECT 
        TO_BINARY('SNOW', 'utf-8') AS utf8_lower,
        TO_BINARY('534E4F57', 'hex') AS hex_lower,
        TO_BINARY('U05PVw==', 'base64') AS base64_lower",
    snapshot_path = "to_binary"
);
