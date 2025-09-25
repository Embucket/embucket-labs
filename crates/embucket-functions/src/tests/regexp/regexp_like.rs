use crate::test_query;

test_query!(
    regexp_like_basic,
    "SELECT REGEXP_LIKE('nevermore1, nevermore2, nevermore3.', 'nevermore')",
    snapshot_path = "regexp_like"
);
test_query!(
    regexp_instr_basic_utf8_view,
    "SELECT REGEXP_LIKE('nevermore1, nevermore2, nevermore3.'::VARCHAR, 'nevermore')",
    snapshot_path = "regexp_like"
);

test_query!(
    regexp_like_null,
    "SELECT * FROM VALUES ('Sacramento'), ('San Francisco'), ('San Jose'), (null) WHERE REGEXP_LIKE(column1, 'san.*')",
    snapshot_path = "regexp_like"
);

test_query!(
    regexp_like_case_insensitive,
    "SELECT * FROM VALUES ('Sacramento'), ('San Francisco'), ('San Jose'), (null) WHERE REGEXP_LIKE(column1, 'san.*', 'i')",
    snapshot_path = "regexp_like"
);
