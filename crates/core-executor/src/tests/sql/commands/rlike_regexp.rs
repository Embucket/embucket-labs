use crate::test_query;

const SETUP_QUERY: &str = r"CREATE OR REPLACE TABLE strings (v VARCHAR(50))
AS SELECT * FROM VALUES
  ('San Francisco'),
  ('San Jose'),
  ('Santa Clara'),
  ('Sacramento'),
  ('Contains embedded single \\\\backslash')";

test_query!(
    rlike_regexp_basic,
    "SELECT v
  FROM strings
  WHERE v REGEXP 'San* [fF].*'
  ORDER BY v",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "rlike_regexp"
);

test_query!(
    rlike_regexp_embedded_backslash,
    r"SELECT v, v REGEXP 'San\\b.*' AS matches
  FROM strings
  ORDER BY v",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "rlike_regexp"
);

test_query!(
    rlike_regexp_embedded_backslashes,
    r"SELECT v, v REGEXP '.*\\s\\\\.*' AS matches
  FROM strings
  ORDER BY v",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "rlike_regexp"
);

test_query!(
    rlike_regexp_string_delimiter,
    r"SELECT v, v REGEXP $$.*\s\\.*$$ AS MATCHES
  FROM strings
  ORDER BY v",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "rlike_regexp"
);
