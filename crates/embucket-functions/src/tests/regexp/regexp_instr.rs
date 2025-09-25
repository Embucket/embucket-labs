use crate::test_query;

test_query!(
    regexp_instr_basic,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d')",
    snapshot_path = "regexp_instr"
);
test_query!(
    regexp_instr_basic_utf8_view,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.'::VARCHAR, 'nevermore\\d')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_position,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d', 5)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_occurrence,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d', 1, 3)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_option_start,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d', 1, 1, 0)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_option_end,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d', 1, 1, 1)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_occurence_limit,
    "SELECT REGEXP_INSTR('nevermore1, nevermore2, nevermore3.', 'nevermore\\d', 1, 4)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_capture_groups,
    "SELECT REGEXP_INSTR(column1, 'the\\W+\\w+')
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_capture_groups_occurrence,
    "SELECT REGEXP_INSTR(column1, 'the\\W+\\w+', 1, 2)
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_capture_groups_group_num,
    "SELECT REGEXP_INSTR(column1, 'the\\W+(\\w+)', 1, 2, 0, 'e', 1)
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_capture_groups_regexp_parameter_no_group_num,
    "SELECT REGEXP_INSTR(column1, 'the\\W+(\\w+)', 1, 2, 0, 'e')
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_capture_groups_group_num_no_regexp_parameter,
    "SELECT REGEXP_INSTR(column1, 'the\\W+(\\w+)', 1, 2, 0, '', 1)
    FROM VALUES ('It was the best of times, it was the worst of times.'),
    ('In    the   string   the   extra   spaces  are   redundant.'),
    ('A thespian theater is nearby.')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_word_groups,
    "SELECT REGEXP_INSTR(column1, 'A\\W+(\\w+)', 1, 1, 0, 'e', 1),
    REGEXP_INSTR(column1, 'A\\W+(\\w+)', 1, 2, 0, 'e', 1),
    REGEXP_INSTR(column1, 'A\\W+(\\w+)', 1, 3, 0, 'e', 1),
    REGEXP_INSTR(column1, 'A\\W+(\\w+)', 1, 4, 0, 'e', 1)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_letter_groups,
    "SELECT REGEXP_INSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 0, 'e', 1),
    REGEXP_INSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 0, 'e', 2),
    REGEXP_INSTR(column1, 'A\\W+(\\w)(\\w)(\\w)', 1, 1, 0, 'e', 3)
    FROM VALUES ('A MAN A PLAN A CANAL')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_word_boundary,
    "SELECT REGEXP_INSTR('It was the best of times, it was the worst of times','\\bwas\\b', 1, 1)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_regex_patterns_1,
    "SELECT REGEXP_INSTR('It was the best of times, it was the worst of times', '[[:alpha:]]{2,}st', 15, 1)",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_regex_patterns_2,
    "SELECT REGEXP_INSTR(column1, '\\b\\S*o\\S*\\b')
    FROM VALUES ('Hellooo World'),
    ('How are you doing today?'),
    ('the quick brown fox jumps over the lazy dog'),
    ('PACK MY BOX WITH FIVE DOZEN LIQUOR JUGS')",
    snapshot_path = "regexp_instr"
);

test_query!(
    regexp_instr_regex_patterns_3,
    "SELECT REGEXP_INSTR(column1, '\\b\\S*o\\S*\\b', 3, 3, 1, 'i')
    FROM VALUES ('Hellooo World'),
    ('How are you doing today?'),
    ('the quick brown fox jumps over the lazy dog'),
    ('PACK MY BOX WITH FIVE DOZEN LIQUOR JUGS')",
    snapshot_path = "regexp_instr"
);
