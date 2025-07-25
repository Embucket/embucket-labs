use crate::test_query;

test_query!(
    to_date_cast_scalar,
    "SELECT '03-April-2024'::DATE",
    snapshot_path = "to_date"
);

test_query!(
    to_date_cast_column,
    "SELECT column1::DATE FROM VALUES ('03-April-2024', '2024-04-03', '04/03/2024')",
    snapshot_path = "to_date"
);
