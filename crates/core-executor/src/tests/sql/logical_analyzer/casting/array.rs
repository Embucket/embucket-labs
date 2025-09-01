use crate::test_query;

test_query!(
    to_array_cast_basic,
    "SELECT to_array('A')::ARRAY",
    snapshot_path = "array"
);

test_query!(
    to_array_cast_null,
    "SELECT to_array(null)::ARRAY",
    snapshot_path = "array"
);

test_query!(
    to_array_cast_multiple,
    "SELECT to_array('A, B, null, 22')::ARRAY",
    snapshot_path = "array"
);