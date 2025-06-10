use crate::test_query;

test_query!(
    basic_monthname,
    "SELECT monthname('2025-06-08T23:39:20.123-07:00'::date) AS value;",
    snapshot_path = "monthname"
);
