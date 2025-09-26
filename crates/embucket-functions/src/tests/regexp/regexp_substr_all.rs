use crate::test_query;

test_query!(
    regexp_substr_all_basic,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', 'a[[:digit:]]')",
    snapshot_path = "regexp_substr_all"
);
test_query!(
    regexp_substr_all_basic_utf8_view,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6'::VARCHAR, 'a[[:digit:]]')",
    snapshot_path = "regexp_substr_all"
);
test_query!(
    regexp_substr_all_position,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', 'a[[:digit:]]', 2)",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_occurrence,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', 'a[[:digit:]]', 1, 3)",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_case_insensitive,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', 'a[[:digit:]]', 1, 1, 'i')",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_case_insensitive_first_group,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', '(a)([[:digit:]])', 1, 1, 'ie')",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_no_match,
    "SELECT REGEXP_SUBSTR_ALL('a1_a2a3_a4A5a6', 'b')",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_word_groups,
    "SELECT REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w+)', 1, 1, 'e', 1),
    REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w+)', 1, 2, 'e', 1),
    REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w+)', 1, 3, 'e', 1)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_substr_all"
);

test_query!(
    regexp_substr_all_letter_groups,
    "SELECT REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 1),
    REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 2),
    REGEXP_SUBSTR_ALL(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 3)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_substr_all"
);
