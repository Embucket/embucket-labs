use crate::test_query;

// SYSTEM$TYPEOF basic casting examples
test_query!(
    system_typeof_basic,
    "SELECT \
        SYSTEM$TYPEOF(CAST('9.8765' AS DECIMAL(5,2))) AS t1, \
        SYSTEM$TYPEOF(CAST('1.2345' AS DECIMAL(6,5))) AS t2, \
        SYSTEM$TYPEOF(CAST(1.2345 AS INTEGER)) AS t3, \
        SYSTEM$TYPEOF(CAST(1.2345 AS VARCHAR)) AS t4, \
        SYSTEM$TYPEOF(CAST('2024-05-09 14:32:29.135 -0700'::TIMESTAMP AS DATE)) AS t5",
    snapshot_path = "system/typeof"
);

// SYSTEM$TYPEOF with literal values
test_query!(
    system_typeof_literals,
    "SELECT \
        SYSTEM$TYPEOF(123) AS t1, \
        SYSTEM$TYPEOF(1.23) AS t2, \
        SYSTEM$TYPEOF('abc') AS t3, \
        SYSTEM$TYPEOF(TRUE) AS t4",
    snapshot_path = "system/typeof"
);

// SYSTEM$TYPEOF on date and timestamp literals
test_query!(
    system_typeof_date,
    "SELECT \
        SYSTEM$TYPEOF(DATE '2024-05-09') AS t1, \
        SYSTEM$TYPEOF(TIMESTAMP '2024-05-09 14:32:29.135') AS t2",
    snapshot_path = "system/typeof"
);

// SYSTEM$TYPEOF on basic expressions
test_query!(
    system_typeof_expressions,
    "SELECT \
        SYSTEM$TYPEOF(1 + 2) AS t1, \
        SYSTEM$TYPEOF(1.5 * 2) AS t2, \
        SYSTEM$TYPEOF(CONCAT('a', 'b')) AS t3",
    snapshot_path = "system/typeof"
);
