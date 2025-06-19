use crate::test_query;

const SETUP_QUERY: [&str; 2] = [
    "CREATE OR REPLACE TABLE testtable (c1 STRING)",
    "INSERT INTO testtable (c1) VALUES ('1'), ('2'), ('3'), ('20'), ('19'), ('18'), ('1'), ('2'), ('3'), ('4'), (NULL), ('30'), (NULL)",
];

test_query!(
    top_basic,
    "SELECT TOP 4 c1 FROM testtable ORDER BY c1",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);

test_query!(
    top_in_subquery,
    "SELECT c1 FROM (SELECT TOP 3 c1 FROM testtable ORDER BY c1) sub ORDER BY c1 DESC",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);

test_query!(
    top_in_cte,
    "WITH cte AS (SELECT TOP 2 c1 FROM testtable ORDER BY c1) SELECT * FROM cte ORDER BY c1",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);

test_query!(
    top_in_union,
    "SELECT c1 FROM (SELECT TOP 1 c1 FROM testtable ORDER BY c1 ASC) UNION ALL (SELECT TOP 1 c1 FROM testtable ORDER BY c1 DESC) ORDER BY c1",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);

test_query!(
    top_with_no_order,
    "SELECT TOP 5 c1 FROM testtable ORDER BY c1",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);

test_query!(
    top_with_limit,
    "SELECT TOP 2 c1 FROM (SELECT TOP 4 c1 FROM testtable ORDER BY c1) sub ORDER BY c1",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "top"
);
