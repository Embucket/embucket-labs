use crate::test_query;

test_query!(
    coercion_utf8_to_boolean,
    "SELECT * FROM VALUES (FALSE), (TRUE) WHERE column1 = 'FALSE'",
    snapshot_path = "custom_type_coercion"
);

test_query!(
    coercion_utf8_invalid_boolean,
    "SELECT * FROM VALUES (FALSE), (TRUE) WHERE column1 = 'TEST'",
    snapshot_path = "custom_type_coercion"
);
