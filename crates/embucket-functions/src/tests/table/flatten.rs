use crate::test_query;

test_query!(
    flatten,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[1,77]')) f;",
    snapshot_path = "flatten"
);
test_query!(
    flatten_non_namedargs,
    "SELECT * FROM FLATTEN('[1,77]') f;",
    snapshot_path = "flatten"
);
test_query!(
    flatten_outer,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88]}'), OUTER => TRUE) f;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_path,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88]}'), PATH => 'b') f;"#,
    snapshot_path = "flatten"
);

test_query!(
    flatten_limit,
    r#"SELECT * from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,true,'both') limit 1;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_projection,
    r#"SELECT d.value from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,true,'both') d;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_projection_alias,
    r#"SELECT d.value as row from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,true,'both') d;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_empty,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[]')) f;",
    snapshot_path = "flatten"
);
test_query!(
    flatten_empty_outer,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[]'), OUTER => TRUE) f;",
    snapshot_path = "flatten"
);
test_query!(
    flatten_non_recursive,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}')) f;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_recursive,
    r#"SELECT * FROM  FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), RECURSIVE => TRUE ) f;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_recursive_mode,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), RECURSIVE => TRUE, MODE => 'OBJECT' ) f;"#,
    snapshot_path = "flatten"
);
test_query!(
    flatten_join,
    "SELECT column1, f.* FROM json_tbl, LATERAL FLATTEN(INPUT => column1, PATH => 'name') f",
    setup_queries = [r#"CREATE TABLE json_tbl AS  SELECT * FROM values
          ('{"name":  {"first": "John", "last": "Smith"}}'),
          ('{"name":  {"first": "Jane", "last": "Doe"}}') v;"#],
    snapshot_path = "flatten"
);
