use crate::test_query;

// Basic SHA2 tests with default digest size (256)
test_query!(
    sha2_default_snowflake_example,
    "SELECT sha2('Snowflake')",
    snapshot_path = "sha2"
);

test_query!(
    sha2_default_abcd0_example,
    "SELECT sha2('AbCd0')",
    snapshot_path = "sha2"
);

// SHA2 with different digest sizes
test_query!(
    sha2_224_snowflake_example,
    "SELECT sha2('Snowflake', 224)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_256_explicit,
    "SELECT sha2('Snowflake', 256)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_384_example,
    "SELECT sha2('Snowflake', 384)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_512_example,
    "SELECT sha2('Snowflake', 512)",
    snapshot_path = "sha2"
);

// SHA2_HEX function (synonym for SHA2)
test_query!(
    sha2_hex_snowflake_example,
    "SELECT sha2_hex('Snowflake')",
    snapshot_path = "sha2"
);

test_query!(
    sha2_hex_224_example,
    "SELECT sha2_hex('Snowflake', 224)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_hex_abcd0_example,
    "SELECT sha2_hex('AbCd0')",
    snapshot_path = "sha2"
);

// Comparison between SHA2 and SHA2_HEX (should be identical)
test_query!(
    sha2_vs_sha2_hex_comparison,
    "SELECT sha2('AbCd0') as sha2_result, sha2_hex('AbCd0') as sha2_hex_result",
    snapshot_path = "sha2"
);

// Different digest sizes comparison
test_query!(
    sha2_all_digest_sizes,
    "SELECT 
        sha2('test', 224) as sha224,
        sha2('test', 256) as sha256,
        sha2('test', 384) as sha384,
        sha2('test', 512) as sha512",
    snapshot_path = "sha2"
);

// NULL input handling
test_query!(
    sha2_null_input,
    "SELECT sha2(NULL) as sha2_null, sha2_hex(NULL) as sha2_hex_null",
    snapshot_path = "sha2"
);

test_query!(
    sha2_null_digest_size,
    "SELECT sha2('test', NULL)",
    snapshot_path = "sha2"
);

// Empty string handling
test_query!(
    sha2_empty_string,
    "SELECT 
        sha2('') as sha2_empty,
        sha2('', 224) as sha2_224_empty,
        sha2('', 384) as sha2_384_empty,
        sha2('', 512) as sha2_512_empty",
    snapshot_path = "sha2"
);

// Various string inputs
test_query!(
    sha2_special_characters,
    "SELECT 
        sha2('Hello World!') as hello_world,
        sha2('123456789') as numbers,
        sha2('Test@#$%^&*()') as special_chars",
    snapshot_path = "sha2"
);

test_query!(
    sha2_unicode_characters,
    "SELECT 
        sha2('Joyeux Noël') as french,
        sha2('圣诞节快乐') as chinese,
        sha2('こんにちは') as japanese,
        sha2('안녕하세요') as korean",
    snapshot_path = "sha2"
);

// Long string test
test_query!(
    sha2_long_string,
    "SELECT sha2('Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.')",
    snapshot_path = "sha2"
);

// Case sensitivity test
test_query!(
    sha2_case_sensitivity,
    "SELECT 
        sha2('hello') as lowercase,
        sha2('HELLO') as uppercase,
        sha2('Hello') as mixed_case",
    snapshot_path = "sha2"
);

// Single character test
test_query!(
    sha2_single_characters,
    "SELECT 
        sha2('a') as char_a,
        sha2('1') as char_1,
        sha2('@') as char_at",
    snapshot_path = "sha2"
);

// Test with whitespace
test_query!(
    sha2_whitespace,
    "SELECT 
        sha2(' ') as single_space,
        sha2('  ') as double_space,
        sha2('\t') as tab,
        sha2('\n') as newline",
    snapshot_path = "sha2"
);

// Error cases - invalid digest sizes
test_query!(
    sha2_invalid_digest_size_128,
    "SELECT sha2('test', 128)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_invalid_digest_size_1024,
    "SELECT sha2('test', 1024)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_invalid_digest_size_zero,
    "SELECT sha2('test', 0)",
    snapshot_path = "sha2"
);

test_query!(
    sha2_invalid_digest_size_negative,
    "SELECT sha2('test', -256)",
    snapshot_path = "sha2"
);

// Test with table data simulation (multiple rows)
test_query!(
    sha2_multiple_rows,
    "SELECT 
        data, 
        sha2(data) as hash_value
    FROM (
        VALUES 
            ('row1'),
            ('row2'), 
            ('row3'),
            (NULL),
            ('')
    ) AS t(data)",
    snapshot_path = "sha2"
);

// Test different digest sizes with same input
test_query!(
    sha2_digest_size_lengths,
    "SELECT 
        sha2('test', 224) as hash_224,
        LENGTH(sha2('test', 224)) as len_224,
        sha2('test', 256) as hash_256,
        LENGTH(sha2('test', 256)) as len_256,
        sha2('test', 384) as hash_384,
        LENGTH(sha2('test', 384)) as len_384,
        sha2('test', 512) as hash_512,
        LENGTH(sha2('test', 512)) as len_512",
    snapshot_path = "sha2"
);

// Verify hex output format (lowercase)
test_query!(
    sha2_hex_format_verification,
    "SELECT 
        sha2('test') as hash,
        LOWER(sha2('test')) = sha2('test') as is_lowercase,
        LENGTH(sha2('test')) as hex_length",
    snapshot_path = "sha2"
);

// Snowflake documentation exact examples
test_query!(
    snowflake_documentation_examples,
    "SELECT 
        sha2('Snowflake', 224) as snowflake_224_exact,
        sha2('AbCd0') as abcd0_256_exact,
        sha2_hex('AbCd0') as abcd0_hex_exact",
    snapshot_path = "sha2"
);
