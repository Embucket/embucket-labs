use crate::test_query;

// Setup table constants
const CREATE_TEST_CHARS: &str =
    "CREATE TABLE test_chars AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(val)";
const CREATE_TEST_WORDS: &str = "CREATE TABLE test_words AS SELECT * FROM (VALUES ('Hello'), ('World'), ('From'), ('LISTAGG')) AS t(val)";
const CREATE_FOOD_ITEMS: &str = "CREATE TABLE food_items AS SELECT * FROM (VALUES (1, 'apple'), (1, 'banana'), (2, 'carrot'), (2, 'broccoli')) AS t(id, name)";
const CREATE_FOOD_ITEMS_UNSORTED: &str = "CREATE TABLE food_items_unsorted AS SELECT * FROM (VALUES (1, 'banana'), (1, 'apple'), (2, 'carrot'), (2, 'broccoli')) AS t(id, name)";
const CREATE_TEST_SEQUENCES: &str = "CREATE TABLE test_sequences AS SELECT * FROM (VALUES (1, 'c'), (1, 'a'), (1, 'b'), (2, 'z'), (2, 'y')) AS t(id, name)";
const CREATE_GROCERY_ITEMS: &str = "CREATE TABLE grocery_items AS SELECT * FROM (VALUES ('fruit', 'apple', 1.20), ('fruit', 'banana', 0.80), ('fruit', 'cherry', 3.50), ('vegetable', 'carrot', 0.90), ('vegetable', 'broccoli', 2.10)) AS t(category, name, price)";
const CREATE_SINGLE_ITEM: &str =
    "CREATE TABLE single_item AS SELECT * FROM (VALUES ('single')) AS t(val)";
const CREATE_MIXED_STRINGS: &str =
    "CREATE TABLE mixed_strings AS SELECT * FROM (VALUES (''), ('a'), (''), ('b')) AS t(val)";
const CREATE_NUMBER_SEQUENCE: &str =
    "CREATE TABLE number_sequence AS SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(num)";

// Basic listagg function tests
test_query!(
    listagg_basic,
    "SELECT LISTAGG(val, ', ') FROM (VALUES ('apple'), ('banana'), ('cherry')) AS t(val)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_with_default_delimiter,
    "SELECT LISTAGG(val) FROM (VALUES ('a'), ('b'), ('c')) AS t(val)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_grouped,
    "SELECT category, LISTAGG(name, ' | ') FROM (VALUES ('fruit', 'apple'), ('fruit', 'banana'), ('vegetable', 'carrot'), ('vegetable', 'broccoli')) AS t(category, name) GROUP BY category ORDER BY category",
    snapshot_path = "listagg"
);

// DISTINCT tests
test_query!(
    listagg_distinct,
    "SELECT LISTAGG(DISTINCT val, ', ') FROM (VALUES ('apple'), ('banana'), ('apple'), ('cherry')) AS t(val)",
    snapshot_path = "listagg"
);

// WITHIN GROUP (ORDER BY) tests
test_query!(
    listagg_within_group_order_by,
    "SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM (VALUES ('cherry'), ('apple'), ('banana')) AS t(name)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_within_group_order_by_desc,
    "SELECT LISTAGG(name, ' -> ') WITHIN GROUP (ORDER BY name DESC) FROM (VALUES ('apple'), ('banana'), ('cherry')) AS t(name)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_distinct_with_order,
    "SELECT LISTAGG(DISTINCT name, ', ') WITHIN GROUP (ORDER BY name) FROM (VALUES ('cherry'), ('apple'), ('banana'), ('apple')) AS t(name)",
    snapshot_path = "listagg"
);

// NULL handling tests
test_query!(
    listagg_with_nulls,
    "SELECT LISTAGG(val, ', ') FROM (VALUES ('apple'), (NULL), ('banana'), (NULL), ('cherry')) AS t(val)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_all_nulls,
    "SELECT LISTAGG(val, ', ') FROM (VALUES (NULL), (NULL), (NULL)) AS t(val)",
    snapshot_path = "listagg"
);

test_query!(
    listagg_empty_input,
    "SELECT LISTAGG(val, ', ') FROM (SELECT * FROM (VALUES ('a')) AS t(val) WHERE val = 'nonexistent')",
    snapshot_path = "listagg"
);

// Different delimiter tests
test_query!(
    listagg_pipe_delimiter,
    "SELECT LISTAGG(val, '|') FROM test_chars",
    setup_queries = [CREATE_TEST_CHARS],
    snapshot_path = "listagg"
);

test_query!(
    listagg_space_delimiter,
    "SELECT LISTAGG(val, ' ') FROM test_words",
    setup_queries = [CREATE_TEST_WORDS],
    snapshot_path = "listagg"
);

// Window function tests
test_query!(
    listagg_window_basic,
    "SELECT id, name, LISTAGG(name, ', ') OVER (PARTITION BY id) AS concatenated FROM food_items ORDER BY id, name",
    setup_queries = [CREATE_FOOD_ITEMS],
    snapshot_path = "listagg"
);

test_query!(
    listagg_window_with_order,
    "SELECT id, name, LISTAGG(name, ' -> ') OVER (PARTITION BY id ORDER BY name) AS concatenated FROM food_items_unsorted ORDER BY id, name",
    setup_queries = [CREATE_FOOD_ITEMS_UNSORTED],
    snapshot_path = "listagg"
);

test_query!(
    listagg_window_running_total,
    "SELECT id, name, LISTAGG(name, ', ') OVER (PARTITION BY id ORDER BY name ROWS UNBOUNDED PRECEDING) AS running_list FROM test_sequences ORDER BY id, name",
    setup_queries = [CREATE_TEST_SEQUENCES],
    snapshot_path = "listagg"
);

// Combined GROUP BY and WITHIN GROUP tests
test_query!(
    listagg_group_by_with_within_group,
    "SELECT category, LISTAGG(name, ', ') WITHIN GROUP (ORDER BY price DESC) AS expensive_first FROM grocery_items GROUP BY category ORDER BY category",
    setup_queries = [CREATE_GROCERY_ITEMS],
    snapshot_path = "listagg"
);

// Edge cases
test_query!(
    listagg_single_value,
    "SELECT LISTAGG(val, ', ') FROM single_item",
    setup_queries = [CREATE_SINGLE_ITEM],
    snapshot_path = "listagg"
);

test_query!(
    listagg_empty_strings,
    "SELECT LISTAGG(val, '|') FROM mixed_strings",
    setup_queries = [CREATE_MIXED_STRINGS],
    snapshot_path = "listagg"
);

// Complex data types
test_query!(
    listagg_numbers_as_strings,
    "SELECT LISTAGG(num, ' + ') FROM number_sequence",
    setup_queries = [CREATE_NUMBER_SEQUENCE],
    snapshot_path = "listagg"
);

// Test DISTINCT with WITHIN GROUP - both must reference the same column (Snowflake requirement)
test_query!(
    listagg_distinct_within_group_same_column,
    "SELECT LISTAGG(DISTINCT name, ' | ') WITHIN GROUP (ORDER BY name) 
     FROM (VALUES ('banana'), ('apple'), ('cherry'), ('apple'), ('banana'), ('date')) AS t(name)",
    snapshot_path = "listagg"
);

// Window functions doesn't support DISTINCT. It's need to be fixed.
/*test_query!(
    listagg_window_distinct,
    "SELECT id, val, LISTAGG(DISTINCT val, ', ') OVER (PARTITION BY id) AS distinct_vals FROM duplicate_values ORDER BY id, val",
    setup_queries = [CREATE_DUPLICATE_VALUES],
    snapshot_path = "listagg"
);*/
