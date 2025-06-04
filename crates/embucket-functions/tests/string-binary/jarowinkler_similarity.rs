use embucket_functions::test_query;

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
