use crate::test_query;

// Basic functionality tests
test_query!(
    hex_decode_binary_basic,
    "SELECT HEX_DECODE_BINARY('534E4F57') AS decoded_value",
    snapshot_path = "hex_decode_binary"
);

test_query!(
    try_hex_decode_binary_basic,
    "SELECT TRY_HEX_DECODE_BINARY('534E4F57') AS decoded_value",
    snapshot_path = "hex_decode_binary"
);

// Known values test
test_query!(
    hex_decode_binary_known_values,
    "SELECT 
        HEX_DECODE_BINARY('48656C6C6F') AS hello_binary,
        HEX_DECODE_BINARY('576F726C64') AS world_binary,
        HEX_DECODE_BINARY('41') AS single_A",
    snapshot_path = "hex_decode_binary"
);

// Empty and null tests
test_query!(
    hex_decode_binary_empty,
    "SELECT HEX_DECODE_BINARY('') AS empty_decoded",
    snapshot_path = "hex_decode_binary"
);

test_query!(
    hex_decode_binary_null,
    "SELECT HEX_DECODE_BINARY(NULL) AS null_decoded",
    snapshot_path = "hex_decode_binary"
);

// Case insensitive test
test_query!(
    hex_decode_binary_case_insensitive,
    "SELECT 
        HEX_DECODE_BINARY('48656c6c6f') AS lowercase_hex,
        HEX_DECODE_BINARY('48656C6C6F') AS uppercase_hex,
        HEX_DECODE_BINARY('48656C6c6F') AS mixed_case_hex",
    snapshot_path = "hex_decode_binary"
);

// Try function with invalid input
test_query!(
    try_hex_decode_binary_invalid_chars,
    "SELECT TRY_HEX_DECODE_BINARY('INVALID') AS should_be_null",
    snapshot_path = "hex_decode_binary"
);

test_query!(
    try_hex_decode_binary_odd_length,
    "SELECT TRY_HEX_DECODE_BINARY('ABC') AS should_be_null",
    snapshot_path = "hex_decode_binary"
);

// Special characters test
test_query!(
    hex_decode_binary_special_chars,
    "SELECT 
        HEX_DECODE_BINARY('21') AS exclamation,
        HEX_DECODE_BINARY('20') AS space,
        HEX_DECODE_BINARY('0A') AS newline,
        HEX_DECODE_BINARY('09') AS tab",
    snapshot_path = "hex_decode_binary"
);

// Unicode test
test_query!(
    hex_decode_binary_unicode,
    "SELECT HEX_DECODE_BINARY('48656C6C6F2C20E4B896E7958C21') AS unicode_hello",
    snapshot_path = "hex_decode_binary"
);

// Long binary test
test_query!(
    hex_decode_binary_long,
    "SELECT HEX_DECODE_BINARY('546865207175696368206272776E20666F78206A756D7073206F76657220746865206C617A7920646F67') AS long_binary",
    snapshot_path = "hex_decode_binary"
);
