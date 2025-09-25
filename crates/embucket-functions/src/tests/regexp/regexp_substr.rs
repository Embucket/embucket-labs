use crate::test_query;

test_query!(
    regexp_substr_basic_scalar,
    "SELECT REGEXP_SUBSTR('nevermore1, nevermore2, nevermore3.', 'nevermore')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_basic_scalar_utf8_view,
    "SELECT REGEXP_SUBSTR('nevermore1, nevermore2, nevermore3.'::VARCHAR, 'nevermore')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_basic_column,
    "SELECT REGEXP_SUBSTR(column1, 'the\\W+\\w+')
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_occurrence,
    "SELECT REGEXP_SUBSTR(column1, 'the\\W+\\w+', 1, 2)
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_group_num,
    "SELECT REGEXP_SUBSTR(column1, 'the\\W+(\\w+)', 1, 2, 'e', 1)
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_word_groups,
    "SELECT REGEXP_SUBSTR(column1, 'A\\W+(\\w+)', 1, 1, 'e', 1),
    REGEXP_SUBSTR(column1, 'A\\W+(\\w+)', 1, 2, 'e', 1),
    REGEXP_SUBSTR(column1, 'A\\W+(\\w+)', 1, 3, 'e', 1),
    REGEXP_SUBSTR(column1, 'A\\W+(\\w+)', 1, 4, 'e', 1)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_letter_groups,
    "SELECT REGEXP_SUBSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 1),
    REGEXP_SUBSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 2),
    REGEXP_SUBSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 'e', 3)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_word_boundary,
    "SELECT REGEXP_SUBSTR('It was the best of times, it was the worst of times','\\bwas\\b', 1, 1)",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_regex_patterns_1,
    "SELECT REGEXP_SUBSTR('It was the best of times, it was the worst of times', '[[:alpha:]]{2,}st', 15, 1)",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_regex_patterns_2,
    "SELECT REGEXP_SUBSTR(column1, '\\b\\S*o\\S*\\b')
    FROM VALUES ('Hellooo World'),
    ('How are you doing today?'),
    ('the quick brown fox jumps over the lazy dog'),
    ('PACK MY BOX WITH FIVE DOZEN LIQUOR JUGS')",
    snapshot_path = "regexp_substr"
);

test_query!(
    regexp_substr_regex_patterns_3,
    "SELECT REGEXP_SUBSTR(column1, '\\b\\S*o\\S*\\b', 3, 3, 'i')
    FROM VALUES ('Hellooo World'),
    ('How are you doing today?'),
    ('the quick brown fox jumps over the lazy dog'),
    ('PACK MY BOX WITH FIVE DOZEN LIQUOR JUGS')",
    snapshot_path = "regexp_substr"
);
