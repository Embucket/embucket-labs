use crate::test_query;

test_query!(
    to_timestamp_scalar,
    "SELECT '03-01-2024'::TIMESTAMP as t",
    snapshot_path = "timestamp"
);

test_query!(
    to_timestamp_column,
    "SELECT column1::TIMESTAMP as t FROM VALUES ('03-Apr-2024 12:12'), ('2024-04-03T12:12:10'), ('04/03/2024T12:12:10')",
    snapshot_path = "timestamp"
);

test_query!(
    to_timestamp_explicit_scalar,
    "SELECT CAST('2024-04-03T12:12:10' AS TIMESTAMP) as t",
    snapshot_path = "timestamp"
);

test_query!(
    to_timestamp_explicit_column,
    "SELECT CAST(column1 AS TIMESTAMP) as t FROM VALUES ('03-Apr-2024 12:12'), ('2024-04-03T12:12:10'), ('04/03/2024T12:12:10')",
    snapshot_path = "timestamp"
);

test_query!(
    to_timestamp_column_already_timestamp,
    "SELECT col::TIMESTAMP as t FROM (SELECT '2024-04-03T12:12:10'::TIMESTAMP AS col)",
    snapshot_path = "timestamp"
);
