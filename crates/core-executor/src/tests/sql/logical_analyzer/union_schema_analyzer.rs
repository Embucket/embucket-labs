use crate::test_query;

// This test ensures that when performing a UNION ALL with a constant timestamp literal
// that has out-of-range nanoseconds (e.g. `'9999-12-31 00:00:00.000 +0000'`),
// the analyzer forces the union schema to use `Timestamp(Microseconds, None)`.
// Without this normalization, schema reconciliation would fail because one side
// might use nanosecond precision while the other provides only microseconds and cannot be cast.
test_query!(
    union_with_timestamp_microseconds,
    "SELECT '2024-12-31 10:00:00.000'::TIMESTAMP as t
        UNION ALL
    SELECT '9999-12-31 00:00:00.000 +0000' AS t
    ORDER BY 1",
    snapshot_path = "union_schema_analyzer"
);

test_query!(
    union_with_timestamp_microseconds_cte,
    "WITH res AS (
        SELECT '2024-12-31 10:00:00.000'::TIMESTAMP as t
        UNION ALL
        SELECT '9999-12-31 00:00:00.000 +0000'
    )
    SELECT t FROM res
    ORDER BY t",
    snapshot_path = "union_schema_analyzer"
);
