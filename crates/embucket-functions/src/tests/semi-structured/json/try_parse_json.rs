use crate::test_query;

test_query!(
    basic_try_parse_json,
    "SELECT try_parse_json('{\"key\": \"value\"}') AS parsed_json",
    snapshot_path = "try_parse_json"
);

test_query!(
    invalid_try_parse_json,
    "SELECT try_parse_json('{\"invalid\": \"json\"') AS parsed_json",
    snapshot_path = "try_parse_json"
);
