use crate::test_query;

test_query!(
    coercion_utf8_to_boolean,
    "SELECT * FROM VALUES (FALSE), (TRUE) WHERE column1 = 'FALSE'",
    snapshot_path = "custom_type_coercion"
);

test_query!(
    coercion_utf8_invalid_boolean,
    "SELECT * FROM VALUES (FALSE), (TRUE) WHERE column1 = 'TEST'",
    snapshot_path = "custom_type_coercion"
);

test_query!(
    coercion_int_division_to_decimal_basic,
    "SELECT 1/2   AS a, 2/4   AS b,  2/4/3.0*100 AS c",
    snapshot_path = "custom_type_coercion"
);

test_query!(
    coercion_int_division_to_decimal_advanced,
    "WITH t AS (SELECT 2 AS sr, 4 AS cr, 8 AS wr) SELECT sr / (sr + cr + wr) / 3.0 * 100 AS pct FROM t",
    snapshot_path = "custom_type_coercion"
);
