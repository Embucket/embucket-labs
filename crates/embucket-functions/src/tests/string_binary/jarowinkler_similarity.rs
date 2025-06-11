use crate::test_query;

// Basic similarity case-insensitive
test_query!(
    basic_similarity,
    "SELECT jarowinkler_similarity('hello', 'HELLO')",
    snapshot_path = "jarowinkler_similarity"
);

// Multiple rows with NULL handling
test_query!(
    multiple_rows,
    "SELECT jarowinkler_similarity(a, b) FROM (\
        VALUES ('Dwayne', 'Duane'),\
               ('martha', 'marhta'),\
               ('hello', 'yellow'),\
               ('foo', NULL),\
               (NULL, 'bar')\
    ) AS t(a, b)",
    snapshot_path = "jarowinkler_similarity"
);

test_query!(
    table_input,
    "SELECT s, t,
       JAROWINKLER_SIMILARITY(s, t),
       JAROWINKLER_SIMILARITY(t, s)
  FROM ed
  ORDER BY s, t;",
    setup_queries = [
        "CREATE OR REPLACE TABLE ed (
  s VARCHAR,
  t VARCHAR
);",
        "INSERT INTO ed (s, t) VALUES
  ('', ''),
  ('Gute nacht', 'Ich weis nicht'),
  ('Ich weiß nicht', 'Ich wei? nicht'),
  ('Ich weiß nicht', 'Ich weiss nicht'),
  ('Ich weiß nicht', NULL),
  ('Snowflake', 'Oracle'),
  ('święta', 'swieta'),
  (NULL, ''),
  (NULL, NULL);"
    ],
    snapshot_path = "jarowinkler_similarity"
);
