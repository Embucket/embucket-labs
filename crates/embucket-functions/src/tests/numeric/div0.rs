use crate::test_query;

// Basic DIV0 functionality
test_query!(
    div0_basic,
    "SELECT DIV0(10, 2), DIV0(10, 0)",
    snapshot_path = "div0"
);

// Different numeric types
test_query!(
    div0_numeric_types,
    "SELECT DIV0(10, 2), DIV0(10.5, 2), DIV0(10, 2.5), DIV0(10.5, 2.5)",
    snapshot_path = "div0"
);

// Division by zero cases
test_query!(
    div0_zero_division,
    "SELECT DIV0(10, 0), DIV0(0, 0), DIV0(-10, 0)",
    snapshot_path = "div0"
);

// NULL handling
test_query!(
    div0_nulls,
    "SELECT DIV0(NULL, 2), DIV0(10, NULL), DIV0(NULL, NULL)",
    snapshot_path = "div0"
);

// Table input
test_query!(
    div0_table_input,
    "SELECT a, b, DIV0(a, b) FROM div0_test ORDER BY a, b",
    setup_queries = [
        "CREATE TABLE div0_test (a INT, b INT)",
        "INSERT INTO div0_test VALUES (10, 2), (10, 0), (NULL, 2), (10, NULL), (NULL, NULL)"
    ],
    snapshot_path = "div0"
);

// Mixed types
test_query!(
    div0_mixed_types,
    "SELECT DIV0(a, b) FROM (VALUES 
        (10, 2), 
        (10.5, 2), 
        (10, 2.5), 
        (10.5, 2.5),
        (10, 0),
        (10.5, 0)
    ) AS t(a, b)",
    snapshot_path = "div0"
);

// Negative numbers
test_query!(
    div0_negative_numbers,
    "SELECT DIV0(-10, 2), DIV0(10, -2), DIV0(-10, -2)",
    snapshot_path = "div0"
);

// Large numbers
test_query!(
    div0_large_numbers,
    "SELECT DIV0(1000000000, 2), DIV0(1000000000, 0)",
    snapshot_path = "div0"
);

// Decimal precision
test_query!(
    div0_decimal_precision,
    "SELECT DIV0(10::DECIMAL(5,3), 3::DECIMAL(6,4)), DIV0(1::DECIMAL(38,15), 3), DIV0(1::DECIMAL(5,3), 300)",
    snapshot_path = "div0"
);

// Expression arguments
test_query!(
    div0_expressions,
    "SELECT DIV0(a + b, c - d) FROM (VALUES (5, 5, 10, 5), (10, 0, 5, 5)) AS t(a, b, c, d)",
    snapshot_path = "div0"
);
