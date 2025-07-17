use crate::test_query;

// Basic functionality tests
test_query!(
    hex_decode_string_basic,
    "SELECT HEX_DECODE_STRING('534E4F57') AS decoded_value",
    snapshot_path = "hex_decode_string"
);

test_query!(
    try_hex_decode_string_basic,
    "SELECT TRY_HEX_DECODE_STRING('534E4F57') AS decoded_value",
    snapshot_path = "hex_decode_string"
);

// Known values test
test_query!(
    hex_decode_string_known_values,
    "SELECT 
        HEX_DECODE_STRING('48656C6C6F') AS hello_binary,
        HEX_DECODE_STRING('576F726C64') AS world_binary,
        HEX_DECODE_STRING('41') AS single_A",
    snapshot_path = "hex_decode_string"
);

// Empty and null tests
test_query!(
    hex_decode_string_empty,
    "SELECT HEX_DECODE_STRING('') AS empty_decoded",
    snapshot_path = "hex_decode_string"
);

test_query!(
    hex_decode_string_null,
    "SELECT HEX_DECODE_STRING(NULL) AS null_decoded",
    snapshot_path = "hex_decode_string"
);

// Case insensitive test
test_query!(
    hex_decode_string_case_insensitive,
    "SELECT 
        HEX_DECODE_STRING('48656c6c6f') AS lowercase_hex,
        HEX_DECODE_STRING('48656C6C6F') AS uppercase_hex,
        HEX_DECODE_STRING('48656C6c6F') AS mixed_case_hex",
    snapshot_path = "hex_decode_string"
);

// Try function with invalid input
test_query!(
    try_hex_decode_string_invalid_chars,
    "SELECT TRY_HEX_DECODE_STRING('INVALID') AS should_be_null",
    snapshot_path = "hex_decode_string"
);

test_query!(
    try_hex_decode_string_odd_length,
    "SELECT TRY_HEX_DECODE_STRING('ABC') AS should_be_null",
    snapshot_path = "hex_decode_string"
);

// Special characters test
test_query!(
    hex_decode_string_special_chars,
    "SELECT 
        HEX_DECODE_STRING('21') AS exclamation,
        HEX_DECODE_STRING('20') AS space,
        HEX_DECODE_STRING('0A') AS newline,
        HEX_DECODE_STRING('09') AS tab",
    snapshot_path = "hex_decode_string"
);

// Unicode test
test_query!(
    hex_decode_string_unicode,
    "SELECT HEX_DECODE_STRING('48656C6C6F2C20E4B896E7958C21') AS unicode_hello",
    snapshot_path = "hex_decode_string"
);

// Long string test
test_query!(
    hex_decode_string_long,
    "SELECT HEX_DECODE_STRING('546865207175696368206272776E20666F78206A756D7073206F76657220746865206C617A7920646F67') AS long_string",
    snapshot_path = "hex_decode_string"
);
