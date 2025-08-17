use crate::test_query;

// Deterministic with fixed seed
test_query!(
    deterministic_basic,
    "SELECT RANDSTR(5, 1234) AS s1, RANDSTR(5, 1234) AS s2",
    snapshot_path = "randstr"
);

// Different seeds produce different outputs
test_query!(
    different_seeds,
    "SELECT RANDSTR(8, 1) AS a, RANDSTR(8, 2) AS b, RANDSTR(8, 3) AS c",
    snapshot_path = "randstr"
);

// Length edge cases: zero length and NULLs
test_query!(
    edge_cases,
    "SELECT RANDSTR(0, 1234) AS zero_len, RANDSTR(NULL, 1234) AS null_len, RANDSTR(4, NULL) AS null_seed",
    snapshot_path = "randstr"
);

// With a table to simulate per-row deterministic results
test_query!(
    per_row,
    "SELECT RANDSTR(6, id) AS v FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY id",
    snapshot_path = "randstr"
);

// Error case: non-numeric value should produce a friendly error
test_query!(
    invalid_string_arg,
    "SELECT RANDSTR('lala', 12)",
    snapshot_path = "randstr"
);
