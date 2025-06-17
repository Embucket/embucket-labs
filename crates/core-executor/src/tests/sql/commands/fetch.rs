use crate::test_query;

const SETUP_QUERIES: [&str; 2] = [
    "CREATE OR REPLACE TABLE fetch_test(c1 INT)",
    "INSERT INTO fetch_test VALUES (1),(2),(3),(4)",
];

test_query!(
    fetch_first_rows_only,
    "SELECT c1 FROM fetch_test FETCH FIRST 2 ROWS ONLY",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch",
);

test_query!(
    fetch_with_offset,
    "SELECT c1 FROM fetch_test OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
    setup_queries = [SETUP_QUERIES[0], SETUP_QUERIES[1]],
    snapshot_path = "fetch",
);
