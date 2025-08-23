use crate::test_query;

test_query!(
    count,
    "SELECT COUNT(*) FROM empty_table",
    setup_queries = ["CREATE TABLE empty_table (id INT)"],
    snapshot_path = "aggregate"
);

test_query!(
    count_cte,
    "WITH c as (SELECT COUNT(*) FROM empty_table) SELECT * FROM c",
    setup_queries = ["CREATE TABLE empty_table (id INT)"],
    snapshot_path = "aggregate"
);
