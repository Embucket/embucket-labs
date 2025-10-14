use crate::test_query;

test_query!(
    int32_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT32)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459'), ('50.49')"
    ],
    snapshot_path = "integer"
);

test_query!(
    int64_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT64)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459'), ('50.49')"
    ],
    snapshot_path = "integer"
);

test_query!(
    int_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459'), ('50.49')"
    ],
    snapshot_path = "integer"
);

test_query!(
    integer_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INTEGER)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459'), ('50.49')"
    ],
    snapshot_path = "integer"
);

test_query!(
    int32_cast_decimal,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT32)",
        "INSERT INTO test VALUES (50), (50.0), (50.9), (50.5), (50.45), (50.459), (50.49)"
    ],
    snapshot_path = "integer"
);

test_query!(
    int64_cast_decimal,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT64)",
        "INSERT INTO test VALUES (50), (50.0), (50.9), (50.5), (50.45), (50.459), (50.49)"
    ],
    snapshot_path = "integer"
);

test_query!(
    int_cast_decimal,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT)",
        "INSERT INTO test VALUES (50), (50.0), (50.9), (50.5), (50.45), (50.459), (50.49)"
    ],
    snapshot_path = "integer"
);

test_query!(
    integer_cast_decimal,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INTEGER)",
        "INSERT INTO test VALUES (50), (50.0), (50.9), (50.5), (50.45), (50.459), (50.49)"
    ],
    snapshot_path = "integer"
);
