use crate::test_query;

test_query!(
    scalar_true,
    "SELECT IFF(TRUE, 'true', 'false');",
    snapshot_path = "iff"
);
test_query!(
    scalar_false,
    "SELECT IFF(FALSE, 'true', 'false');",
    snapshot_path = "iff"
);

// Division by zero cases
test_query!(
    expr,
    "SELECT value, IFF(value::INT = value, 'integer', 'non-integer') 
    FROM ( SELECT column1 AS value FROM VALUES(1.0), (1.1), (-3.1415), (-5.000), (NULL) ) 
    ORDER BY value DESC;;",
    snapshot_path = "iff"
);

test_query!(
    expr_condition,
    "SELECT value, IFF(value > 50, 'High', 'Low')
     FROM ( SELECT column1 AS value FROM VALUES(22), (63), (5), (99), (NULL) )",
    snapshot_path = "iff"
);
