use crate::test_query;

test_query!(
    decrypt_raw_roundtrip_aes128,
    "WITH encrypted AS (
        SELECT ENCRYPT_RAW(
            TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
            TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
        ) AS result
    ),
    parsed AS (
        SELECT 
            PARSE_JSON(result) AS json_result,
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX') AS key
        FROM encrypted
    )
    SELECT 
        DECRYPT_RAW(
            TO_BINARY(GET(json_result, 'ciphertext')::VARCHAR, 'HEX'),
            key,
            TO_BINARY(GET(json_result, 'iv')::VARCHAR, 'HEX'),
            TO_BINARY('', 'HEX'),  -- empty AAD
            'AES-GCM',
            TO_BINARY(GET(json_result, 'tag')::VARCHAR, 'HEX')
        ) AS decrypted
    FROM parsed",
    snapshot_path = "encryption"
);

// Test round-trip encryption/decryption with AES-256
test_query!(
    decrypt_raw_roundtrip_aes256,
    "WITH encrypted AS (
        SELECT ENCRYPT_RAW(
            TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 32-byte key
            TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
        ) AS result
    ),
    parsed AS (
        SELECT 
            PARSE_JSON(result) AS json_result,
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', 'HEX') AS key
        FROM encrypted
    )
    SELECT 
        DECRYPT_RAW(
            TO_BINARY(GET(json_result, 'ciphertext')::VARCHAR, 'HEX'),
            key,
            TO_BINARY(GET(json_result, 'iv')::VARCHAR, 'HEX'),
            TO_BINARY('', 'HEX'),  -- empty AAD
            'AES-GCM',
            TO_BINARY(GET(json_result, 'tag')::VARCHAR, 'HEX')
        ) AS decrypted
    FROM parsed",
    snapshot_path = "encryption"
);

// Test round-trip with Additional Authenticated Data (AAD)
test_query!(
    decrypt_raw_roundtrip_with_aad,
    "WITH encrypted AS (
        SELECT ENCRYPT_RAW(
            TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
            TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
            TO_BINARY('6164646974696F6E616C2064617461', 'HEX')  -- 'additional data' in hex
        ) AS result
    ),
    parsed AS (
        SELECT 
            PARSE_JSON(result) AS json_result,
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX') AS key,
            TO_BINARY('6164646974696F6E616C2064617461', 'HEX') AS aad
        FROM encrypted
    )
    SELECT 
        DECRYPT_RAW(
            TO_BINARY(GET(json_result, 'ciphertext')::VARCHAR, 'HEX'),
            key,
            TO_BINARY(GET(json_result, 'iv')::VARCHAR, 'HEX'),
            aad,
            'AES-GCM',
            TO_BINARY(GET(json_result, 'tag')::VARCHAR, 'HEX')
        ) AS decrypted
    FROM parsed",
    snapshot_path = "encryption"
);

// Test with NULL values for ciphertext and key (should return null)
test_query!(
    decrypt_raw_null_values,
    "SELECT 
        DECRYPT_RAW(NULL, TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'), TO_BINARY('416C736F4E6F745365637265', 'HEX'), TO_BINARY('', 'HEX'), 'AES-GCM', TO_BINARY('00112233445566778899AABBCCDDEEFF', 'HEX')) AS null_ciphertext,
        DECRYPT_RAW(TO_BINARY('DEADBEEF', 'HEX'), NULL, TO_BINARY('416C736F4E6F745365637265', 'HEX'), TO_BINARY('', 'HEX'), 'AES-GCM', TO_BINARY('00112233445566778899AABBCCDDEEFF', 'HEX')) AS null_key",
    snapshot_path = "encryption"
);

// Test error case - null IV (should return error)
test_query!(
    decrypt_raw_null_iv_error,
    "SELECT 
        DECRYPT_RAW(TO_BINARY('DEADBEEF', 'HEX'), TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'), NULL, TO_BINARY('', 'HEX'), 'AES-GCM', TO_BINARY('00112233445566778899AABBCCDDEEFF', 'HEX')) AS null_iv",
    snapshot_path = "encryption"
);

// Test error cases - invalid key length
test_query!(
    decrypt_raw_invalid_key_length,
    "SELECT DECRYPT_RAW(
        TO_BINARY('DEADBEEF', 'HEX'),  -- dummy ciphertext
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF01', 'HEX'),  -- 17-byte key (invalid)
        TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
        TO_BINARY('', 'HEX'),  -- empty AAD
        'AES-GCM',
        TO_BINARY('00112233445566778899AABBCCDDEEFF', 'HEX')  -- dummy tag
    ) AS decrypted",
    snapshot_path = "encryption"
);

// Test error cases - unsupported encryption method
test_query!(
    decrypt_raw_unsupported_method,
    "SELECT DECRYPT_RAW(
        TO_BINARY('DEADBEEF', 'HEX'),  -- dummy ciphertext
        TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
        TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
        TO_BINARY('', 'HEX'),  -- empty AAD
        'AES-CBC',  -- unsupported method
        TO_BINARY('00112233445566778899AABBCCDDEEFF', 'HEX')  -- dummy tag
    ) AS decrypted",
    snapshot_path = "encryption"
);

// Test error case - wrong AAD (should fail authentication)
test_query!(
    decrypt_raw_wrong_aad,
    "WITH encrypted AS (
        SELECT ENCRYPT_RAW(
            TO_BINARY('426F6E6A6F7572', 'HEX'),  -- 'Bonjour' in hex
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
            TO_BINARY('416C736F4E6F745365637265', 'HEX'),  -- 12-byte IV (24 hex chars)
            TO_BINARY('6164646974696F6E616C2064617461', 'HEX')  -- 'additional data' in hex
        ) AS result
    ),
    parsed AS (
        SELECT 
            PARSE_JSON(result) AS json_result,
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX') AS key,
            TO_BINARY('77726F6E672061616420646174610000', 'HEX') AS wrong_aad  -- different AAD
        FROM encrypted
    )
    SELECT 
        DECRYPT_RAW(
            TO_BINARY(GET(json_result, 'ciphertext')::VARCHAR, 'HEX'),
            key,
            TO_BINARY(GET(json_result, 'iv')::VARCHAR, 'HEX'),
            wrong_aad,  -- using wrong AAD
            'AES-GCM',
            TO_BINARY(GET(json_result, 'tag')::VARCHAR, 'HEX')
        ) AS decrypted
    FROM parsed",
    snapshot_path = "encryption"
);

// Test with empty data round-trip
test_query!(
    decrypt_raw_empty_data_roundtrip,
    "WITH encrypted AS (
        SELECT ENCRYPT_RAW(
            TO_BINARY('', 'HEX'),  -- empty data
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX'),  -- 16-byte key
            TO_BINARY('416C736F4E6F745365637265', 'HEX')  -- 12-byte IV (24 hex chars)
        ) AS result
    ),
    parsed AS (
        SELECT 
            PARSE_JSON(result) AS json_result,
            TO_BINARY('0123456789ABCDEF0123456789ABCDEF', 'HEX') AS key
        FROM encrypted
    )
    SELECT 
        DECRYPT_RAW(
            TO_BINARY(GET(json_result, 'ciphertext')::VARCHAR, 'HEX'),
            key,
            TO_BINARY(GET(json_result, 'iv')::VARCHAR, 'HEX'),
            TO_BINARY('', 'HEX'),  -- empty AAD
            'AES-GCM',
            TO_BINARY(GET(json_result, 'tag')::VARCHAR, 'HEX')
        ) AS decrypted
    FROM parsed",
    snapshot_path = "encryption"
);
