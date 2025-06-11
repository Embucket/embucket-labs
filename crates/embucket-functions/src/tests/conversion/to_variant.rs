use crate::test_query;

test_query!(
    basic_to_variant,
    "SELECT to_variant('{\"key\": \"value\"}') AS variant",
    snapshot_path = "to_variant"
);

test_query!(
    null_to_variant,
    "SELECT to_variant(NULL) AS variant",
    snapshot_path = "to_variant"
);
