use crate::test_query;

test_query!(
    timestamp_scale,
    r#"SELECT
       TO_TIMESTAMP(1000000000, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000, 9) AS "Scale in nanoseconds";"#,
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_scaled,
    r#"SELECT
       TO_TIMESTAMP(1000000000) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000, 9) AS "Scale in nanoseconds";"#,
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_scale_decimal,
    r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#,
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_scale_decimal_scaled,
    r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#,
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_scale_int_str,
    r#"SELECT
       TO_TIMESTAMP('1000000000') AS "Scale in seconds",
       TO_TIMESTAMP('1000000000000') AS "Scale in milliseconds",
       TO_TIMESTAMP('1000000000000000') AS "Scale in microseconds",
       TO_TIMESTAMP('1000000000000000000') AS "Scale in nanoseconds";"#,
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_str_format,
    "SELECT
       TO_TIMESTAMP('04/05/2024 01:02:03', 'mm/dd/yyyy hh24:mi:ss') as a,
       TO_TIMESTAMP('04/05/2024 01:02:03') as b",
    setup_queries = ["SET timestamp_input_format = 'mm/dd/yyyy hh24:mi:ss'"],
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_timestamp,
    "SELECT TO_TIMESTAMP(1000000000::TIMESTAMP)",
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_date,
    "SELECT TO_TIMESTAMP('2022-01-01 11:30:00'::date)",
    snapshot_path = "to_timestamp"
);

test_query!(
    timestamp_timezone,
    "SELECT TO_TIMESTAMP(1000000000)",
    setup_queries = ["ALTER SESSION SET timestamp_input_mapping = 'timestamp_tz'"],
    snapshot_path = "to_timestamp"
);
