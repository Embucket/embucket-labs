use crate::test_query;

// Basic string length tests
test_query!(
    basic_string_length,
    "SELECT LENGTH('hello'), LENGTH(''), LENGTH(NULL)",
    snapshot_path = "length"
);

// UTF-8 multi-byte character tests
test_query!(
    utf8_multibyte_characters,
    "SELECT 
        LENGTH('Joyeux Noël') AS french,
        LENGTH('圣诞节快乐') AS chinese,
        LENGTH('こんにちは') AS japanese,
        LENGTH('안녕하세요') AS korean",
    snapshot_path = "length"
);
