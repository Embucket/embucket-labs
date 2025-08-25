use crate::test_query;

test_query!(
    timestamp_scale,
    r#"SELECT
       TO_TIMESTAMP(1000000000, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000, 9) AS "Scale in nanoseconds";"#
);

test_query!(
    timestamp_scaled,
    r#"SELECT
       TO_TIMESTAMP(1000000000) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000, 9) AS "Scale in nanoseconds";"#
);

test_query!(
    timestamp_scale_decimal,
    r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#
);

test_query!(
    timestamp_scale_decimal_scaled,
    r#"SELECT
       TO_TIMESTAMP(1000000000::DECIMAL, 0) AS "Scale in seconds",
       TO_TIMESTAMP(1000000000000::DECIMAL, 3) AS "Scale in milliseconds",
       TO_TIMESTAMP(1000000000000000::DECIMAL, 6) AS "Scale in microseconds",
       TO_TIMESTAMP(1000000000000000000::DECIMAL, 9) AS "Scale in nanoseconds";"#
);

test_query!(
    timestamp_scale_int_str,
    r#"SELECT
       TO_TIMESTAMP('1000000000') AS "Scale in seconds",
       TO_TIMESTAMP('1000000000000') AS "Scale in milliseconds",
       TO_TIMESTAMP('1000000000000000') AS "Scale in microseconds",
       TO_TIMESTAMP('1000000000000000000') AS "Scale in nanoseconds";"#
);

test_query!(
    timestamp_timestamp,
    "SELECT TO_TIMESTAMP(1000000000::TIMESTAMP) as t"
);

test_query!(
    timestamp_date,
    "SELECT TO_TIMESTAMP('2022-01-01 11:30:00'::date) as t"
);

test_query!(
    timestamp_out_nanos_range,
    "SELECT
        '9999-12-31 00:00:02'::TIMESTAMP as t,
        '9999-12-31 00:00:02.000912'::TIMESTAMP as t2,
        '9999-12-31 00:00:02.000912123'::TIMESTAMP as t3,
        '31-Dec-9999 00:00:02.000912123'::TIMESTAMP as t4,
        '31-12-9999 00:00:02.000912123'::TIMESTAMP as t5"
);
