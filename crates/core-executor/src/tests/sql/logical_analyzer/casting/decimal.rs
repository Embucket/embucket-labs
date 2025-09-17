use crate::test_query;

test_query!(
    decimal_cast_integer,
    "SELECT column1::NUMBER as v FROM VALUES (50), (60)",
    snapshot_path = "decimal"
);

test_query!(
    decimal_cast_bool,
    "SELECT column1::NUMBER as v FROM VALUES (FALSE), (TRUE)",
    snapshot_path = "decimal"
);
