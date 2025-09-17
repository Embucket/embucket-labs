use crate::test_query;

test_query!(
    coercion_utf8_yo_boolean,
    "SELECT * FROM VALUES (FALSE), (TRUE) WHERE column1 = 'FALSE'",
    snapshot_path = "custom_type_coercion"
);
