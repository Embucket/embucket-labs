use crate::test_query;

test_query!(
    to_date_cast_scalar,
    "SELECT '03-April-2024'::DATE",
    snapshot_path = "date"
);

test_query!(
    to_date_cast_column,
    "SELECT column1::DATE FROM VALUES ('03-April-2024'), ('2024-04-03'), ('04/03/2024')",
    snapshot_path = "date"
);

test_query!(
    to_date_cast_explicit_scalar,
    "SELECT CAST('03-April-2024' AS DATE)",
    snapshot_path = "date"
);

test_query!(
    to_date_cast_explicit_column,
    "SELECT CAST(column1 AS DATE) FROM VALUES ('03-April-2024'), ('2024-04-03'), ('04/03/2024')",
    snapshot_path = "date"
);

test_query!(
    to_date_cast_column_already_date,
    "SELECT col::DATE FROM (SELECT '2024-04-03'::DATE AS col)",
    snapshot_path = "date"
);
