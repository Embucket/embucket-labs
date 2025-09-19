use crate::test_query;

test_query!(
    md5,
    "SELECT
        MD5('Snowflake') as s,
        MD5('123') as si,
        MD5(123) as i,
        MD5(123.12) as f;",
    snapshot_path = "md5"
);

test_query!(
    md5_hex,
    "SELECT
        MD5_HEX('Snowflake') as s,
        MD5_HEX('123') as si,
        MD5_HEX(123) as i,
        MD5_HEX(123.12) as f;",
    snapshot_path = "md5"
);

test_query!(
    md5_column,
    "SELECT MD5(col_str) AS c1, MD5(CAST(col_int AS VARCHAR)) AS c2
    FROM VALUES 
        ('snow', 'flake'),
        ('1', '33')
    AS t(col_str, col_int);",
    snapshot_path = "md5"
);
