use crate::test_query;

test_query!(
    replace,
    "SELECT REPLACE('down', 'down', 'up');",
    snapshot_path = "replace"
);
test_query!(
    replace_utf8_view,
    "SELECT REPLACE('down'::VARCHAR, 'down', 'up');",
    snapshot_path = "replace"
);

test_query!(
    replace_case,
    "SELECT REPLACE('Vacation in Athens', 'Athens', 'Rome');",
    snapshot_path = "replace"
);

test_query!(
    replace_empty_pattern,
    "SELECT REPLACE('abcd', 'bc');",
    snapshot_path = "replace"
);

test_query!(
    replace_columns,
    "SELECT REPLACE(a, b, c) FROM (
        VALUES ('Dwayne', 'Dway', 'Jon'),
               ('martha', 'tha', 'da'),
               ('hello', 'yellow', 'green'),
               ('foo', NULL, 'test'),
               (NULL, 'bar', 'ban')
    ) AS t(a, b, c)",
    snapshot_path = "replace"
);
