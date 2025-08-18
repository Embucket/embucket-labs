use crate::test_query;

test_query!(
    object_to_object,
    "SELECT to_object('{\"key\": \"value\"}')",
    snapshot_path = "to_object"
);

test_query!(
    null_to_object,
    "SELECT to_object(null)",
    snapshot_path = "to_object"
);

test_query!(
    int_to_object,
    "SELECT to_object(1)",
    snapshot_path = "to_object"
);
