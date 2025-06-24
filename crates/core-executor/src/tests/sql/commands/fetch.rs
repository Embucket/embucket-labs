use crate::test_query;

const SETUP_QUERIES: [&str; 2] = [
    "CREATE OR REPLACE TABLE fetch_test(c1 INT)",
    "INSERT INTO fetch_test VALUES (1),(2),(3),(4)",
];

test_query!(
    fetch_first_rows_only,
    "SELECT c1 FROM fetch_test ORDER BY c1 FETCH FIRST 2 ROWS",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch"
);

test_query!(
    fetch_with_offset,
    "SELECT c1 FROM fetch_test ORDER BY c1 OFFSET 1 ROWS FETCH NEXT 2 ROWS",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch"
);

test_query!(
    fetch_with_cte,
    "WITH limited_data AS (
        SELECT c1 FROM fetch_test ORDER BY c1 FETCH FIRST 3 ROWS
    )
    SELECT * FROM limited_data ORDER BY c1",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch"
);

test_query!(
    fetch_in_union_subquery,
    "SELECT c1, 'first_two' as source FROM (
        SELECT c1 FROM fetch_test ORDER BY c1 FETCH FIRST 2 ROWS
    ) t1
    UNION ALL
    SELECT c1, 'last_two' as source FROM (
        SELECT c1 FROM fetch_test ORDER BY c1 FETCH NEXT 2 ROWS 
    ) t2
    ORDER BY c1, source",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch"
);
