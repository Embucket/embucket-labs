use crate::test_query;

test_query!(
    list_field_metadata_array,
    "SELECT * FROM array",
    setup_queries = ["CREATE TABLE array as (SELECT [1,2]::ARRAY)"],
    snapshot_path = "list_field_metadata"
);

test_query!(
    list_field_metadata_literal,
    "SELECT * FROM array",
    setup_queries = ["CREATE TABLE array as (SELECT 'A'::ARRAY)"],
    snapshot_path = "list_field_metadata"
);

test_query!(
    list_field_metadata_arrays_column,
    "SELECT * FROM array",
    setup_queries =
        ["CREATE TABLE array as (SELECT arr FROM VALUES ([1,2]::ARRAY),([1,2]::ARRAY) AS t(arr))"],
    snapshot_path = "list_field_metadata"
);

test_query!(
    list_field_metadata_arrays_with_column_cast,
    "SELECT * FROM array",
    setup_queries =
        ["CREATE TABLE array as (SELECT arr::ARRAY as arr FROM VALUES ([1,2]),([1,2]) AS t(arr))"],
    snapshot_path = "list_field_metadata"
);
