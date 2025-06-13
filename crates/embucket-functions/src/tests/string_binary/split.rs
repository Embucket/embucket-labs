use crate::test_query;

test_query!(
    basic_split,
    "SELECT split(a, b) FROM (\
        VALUES ('hello world', ' '),\
               ('a.b.c', '.'),\
               ('abc', ','),\
               (NULL, '.'),\
               ('a', NULL)\
    ) AS t(a, b)",
    snapshot_path = "split"
);
