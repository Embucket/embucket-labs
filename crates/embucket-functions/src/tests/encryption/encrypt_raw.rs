use crate::test_query;

// Basic ENCRYPT_RAW tests with AES-128 (16-byte key)
test_query!(
    basic_encrypt_raw_aes128,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
    ) AS encrypted",
    snapshot_path = "encryption"
);

// ENCRYPT_RAW with AES-256 (32-byte key)
test_query!(
    encrypt_raw_aes256,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 32-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
    ) AS encrypted",
    snapshot_path = "encryption"
);

// ENCRYPT_RAW with Additional Authenticated Data (AAD)
test_query!(
    encrypt_raw_with_aad,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
        TO_BINARY('6164646974696F6E616C2064617461', 'HEX')  -- 'additional data' in hex
    ) AS encrypted",
    snapshot_path = "encryption"
);

// ENCRYPT_RAW with explicit AES-GCM method
test_query!(
    encrypt_raw_explicit_method,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
        TO_BINARY('6164646974696F6E616C2064617461', 'HEX'),  -- 'additional data' in hex
        'AES-GCM'
    ) AS encrypted",
    snapshot_path = "encryption"
);

// Test with NULL values. Don't test null IV here because it's not deterministic.
test_query!(
    encrypt_raw_null_values,
    "SELECT 
        ENCRYPT_RAW(NULL, TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'), TO_BINARY('416C736F4E6F745365637265', 'HEX')) AS null_data,
        ENCRYPT_RAW(TO_BINARY('426F6E6A6F7572', 'HEX'), NULL, TO_BINARY('416C736F4E6F745365637265', 'HEX')) AS null_key",
    snapshot_path = "encryption"
);

// Test with empty binary data
test_query!(
    encrypt_raw_empty_data,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('', 'HEX'),  -- empty data
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
    ) AS encrypted",
    snapshot_path = "encryption"
);

// Test error cases - invalid key length (24 bytes, should now work with AES-192)
test_query!(
    encrypt_raw_aes192_key,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF01234567', 'HEX'),  -- 24-byte key (AES-192)
        TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
    ) AS encrypted",
    snapshot_path = "encryption"
);

// Test error cases - unsupported encryption method
test_query!(
    encrypt_raw_unsupported_method,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
        TO_BINARY('6164646974696F6E616C2064617461', 'HEX'),  -- 'additional data' in hex
        'AES-CBC'  -- unsupported method
    ) AS encrypted",
    snapshot_path = "encryption"
);

// Test with very short IV (should fail)
test_query!(
    encrypt_raw_short_iv,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F7453', 'HEX')  -- 8-byte IV (too short for GCM)
    ) AS encrypted",
    snapshot_path = "encryption"
);

// Test with longer text data
test_query!(
    encrypt_raw_longer_data,
    "SELECT ENCRYPT_RAW(
        TO_BINARY('48656C6C6F20576F726C642120546869732069732061206C6F6E6765722074657374206D6573736167652E', 'HEX'),  -- 'Hello World! This is a longer test message.'
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
    ) AS encrypted",
    snapshot_path = "encryption"
);
