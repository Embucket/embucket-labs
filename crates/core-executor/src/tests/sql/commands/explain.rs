use crate::test_query;

const SETUP_QUERY: &str = r"CREATE OR REPLACE TABLE testing (name VARCHAR)
AS SELECT * FROM VALUES
  ('yes'),
  ('no')";

test_query!(
    explain_visitors_basic,
    "EXPLAIN SELECT dateadd(day, 30, '20-12-2022');",
    snapshot_path = "explain"
);

test_query!(
    explain_analyze_visitors_basic,
    "EXPLAIN ANALYZE SELECT dateadd(day, 30, '20-12-2022');",
    snapshot_path = "explain"
);

test_query!(
    explain_verbose_visitors_basic,
    "EXPLAIN VERBOSE SELECT dateadd(day, 30, '20-12-2022');",
    snapshot_path = "explain"
);

test_query!(
    explain_references_basic,
    "EXPLAIN SELECT name FROM testing;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "explain"
);

test_query!(
    explain_analyze_references_basic,
    "EXPLAIN ANALYZE SELECT name FROM testing;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "explain"
);

test_query!(
    explain_verbose_references_basic,
    "EXPLAIN VERBOSE SELECT name FROM testing;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "explain"
);
