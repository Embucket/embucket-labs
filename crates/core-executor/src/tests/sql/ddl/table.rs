use crate::test_query;

test_query!(
    create_table_with_timestamps,
    "SELECT * FROM timestamps",
   setup_queries = [
        "CREATE TABLE timestamps (
            ntz TIMESTAMP_NTZ, ntz_0 TIMESTAMP_NTZ(0), ntz_3 TIMESTAMP_NTZ(3), ntz_6 TIMESTAMP_NTZ(6), ntz_9 TIMESTAMP_NTZ(9),
            ltz TIMESTAMP_LTZ, ltz_0 TIMESTAMP_LTZ(0), ltz_3 TIMESTAMP_LTZ(3), ltz_6 TIMESTAMP_LTZ(6), ltz_9 TIMESTAMP_LTZ(9),
            tz TIMESTAMP_TZ, tz_0 TIMESTAMP_TZ(0), tz_3 TIMESTAMP_TZ(3), tz_6 TIMESTAMP_TZ(6), tz_9 TIMESTAMP_TZ(9),
            dt DATETIME, dt_0 DATETIME(0), dt_3 DATETIME(3), dt_6 DATETIME(6), dt_9 DATETIME(9))
        as VALUES (
            '2025-04-09T21:11:23','2025-04-09T22:11:23','2025-04-09T23:11:23','2025-04-09T20:11:23','2025-04-09T19:11:23',
            '2025-04-09T21:11:23','2025-04-09T22:11:23','2025-04-09T23:11:23','2025-04-09T20:11:23','2025-04-09T19:11:23',
            '2025-04-09T21:11:23','2025-04-09T22:11:23','2025-04-09T23:11:23','2025-04-09T20:11:23','2025-04-09T19:11:23',
            '2025-04-09T21:11:23','2025-04-09T22:11:23','2025-04-09T23:11:23','2025-04-09T20:11:23','2025-04-09T19:11:23'
        );"
    ],
    snapshot_path = "table"
);

test_query!(
    create_table_and_insert,
    "SELECT * FROM embucket.public.test",
    setup_queries = [
        "CREATE TABLE embucket.public.test (id INT)",
        "INSERT INTO embucket.public.test VALUES (1), (2)",
    ],
    snapshot_path = "table"
);

test_query!(
    create_table_quoted_identifiers,
    "SELECT * FROM embucket.\"test public\".\"test table\"",
    setup_queries = [
        "CREATE SCHEMA embucket.\"test public\"",
        "CREATE TABLE embucket.\"test public\".\"test table\" (id INT)",
        "INSERT INTO embucket.\"test public\".\"test table\" VALUES (1), (2)",
    ],
    snapshot_path = "table"
);

// CREATE TABLE with casting timestamp nanosecond to iceberg timestamp microseconds
test_query!(
    create_table_with_casting_timestamp,
    "CREATE OR REPLACE TABLE t1 AS
        SELECT * FROM (VALUES ('2021-03-02 15:55:18.539000'::TIMESTAMP)) AS t(start_tstamp);",
    snapshot_path = "table"
);

test_query!(
    drop_table,
    "SHOW TABLES IN public STARTS WITH 'test'",
    setup_queries = [
        "CREATE TABLE embucket.public.test (id INT) as VALUES (1), (2)",
        "DROP TABLE embucket.public.test"
    ],
    snapshot_path = "table"
);

test_query!(
    drop_table_quoted_identifiers,
    "SHOW TABLES IN public STARTS WITH 'test'",
    setup_queries = [
        "CREATE SCHEMA embucket.\"test public\"",
        "CREATE TABLE embucket.\"test public\".\"test table\" (id INT)",
        "INSERT INTO embucket.\"test public\".\"test table\" VALUES (1), (2)",
        "DROP TABLE embucket.\"test public\".\"test table\"",
    ],
    snapshot_path = "table"
);
