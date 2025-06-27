use crate::test_query;

test_query!(
    flatten,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[1,77]')) f;"
);
test_query!(
    flatten_outer,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88]}'), OUTER => TRUE) f;"#
);

test_query!(
    flatten_path,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88]}'), PATH => 'b') f;"#
);
test_query!(
    flatten_empty,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[]')) f;"
);

test_query!(
    flatten_empty_outer,
    "SELECT * FROM FLATTEN(INPUT => PARSE_JSON('[]'), OUTER => TRUE) f;"
);

test_query!(
    flatten_non_recursive,
    r#"SELECT * FROM FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}')) f;"#
);

test_query!(
    flatten_recursive,
    r#"SELECT * FROM
    FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), RECURSIVE => TRUE ) f;"#
);

test_query!(
    flatten_recursive_mode,
    r#"SELECT * FROM
    FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88], "c": {"d":"X"}}'), RECURSIVE => TRUE, MODE => 'OBJECT' ) f;"#
);

test_query!(
    flatten_join,
    "SELECT column1, f.* FROM json_tbl, LATERAL FLATTEN(INPUT => column1, PATH => 'name') f",
    setup_queries = [r#"CREATE TABLE json_tbl AS  SELECT * FROM values
          ('{"name":  {"first": "John", "last": "Smith"}}'),
          ('{"name":  {"first": "Jane", "last": "Doe"}}') v;"#]
);
