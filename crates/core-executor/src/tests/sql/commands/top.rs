use crate::test_query;

const SETUP_QUERY: [&str; 2] = [
    "CREATE OR REPLACE TABLE testtable (c1 STRING)",
    "INSERT INTO testtable (c1) VALUES ('1'), ('2'), ('3'), ('20'), ('19'), ('18'), ('1'), ('2'), ('3'), ('4'), (NULL), ('30'), (NULL)",
];

test_query!(
    top_basic,
    "SELECT TOP 4 c1 FROM testtable",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top",
);
