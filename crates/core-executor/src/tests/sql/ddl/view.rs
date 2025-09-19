use crate::test_query;

test_query!(
    view_basic_creation,
    "SELECT * FROM view_test ORDER BY id",
    setup_queries = ["CREATE VIEW view_test AS SELECT 1 AS id, 'Alice' AS name"],
    snapshot_path = "view"
);

test_query!(
    temporary_view,
    "SELECT * FROM view_test ORDER BY id",
    setup_queries = ["CREATE TEMPORARY VIEW view_test AS SELECT 1 AS id, 'Alice' AS name"],
    snapshot_path = "view"
);

test_query!(
    create_or_replace_view,
    "SELECT * FROM view",
    setup_queries = [
        "CREATE VIEW view AS SELECT 1 as val;",
        "CREATE OR REPLACE VIEW view AS
        SELECT * FROM (VALUES ('2021-03-02 15:55:18.539000'::TIMESTAMP)) AS t(start_tstamp);"
    ],
    snapshot_path = "view"
);

test_query!(
    view_from_table,
    "SELECT * FROM view ORDER BY id",
    setup_queries = [
        "CREATE TABLE view_source (ID INTEGER, name VARCHAR)",
        "CREATE OR REPLACE VIEW view AS SELECT * FROM view_source",
        "INSERT INTO view_source VALUES (1, 'Alice'), (2, 'Bob')",
    ],
    snapshot_path = "view"
);
