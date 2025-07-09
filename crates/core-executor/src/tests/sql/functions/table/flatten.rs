use crate::test_query;

test_query!(
    flatten_cte,
    r#"WITH base AS (SELECT '{"a": 1}' AS jsontext),
        intermediate AS (SELECT value FROM base, LATERAL FLATTEN(INPUT => parse_json(jsontext)) d)
    SELECT * FROM intermediate;"#,
    snapshot_path = "flatten"
);
