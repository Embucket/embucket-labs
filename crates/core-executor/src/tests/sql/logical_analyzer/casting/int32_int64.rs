use crate::test_query;

test_query!(
    in32_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT32)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459')"
    ],
    snapshot_path = "int32_int64"
);

test_query!(
    in64_cast,
    "SELECT * FROM test",
    setup_queries = [
        "CREATE TABLE test (a INT64)",
        "INSERT INTO test VALUES ('50'), ('50.0'), ('50.9'), ('50.5'), ('50.45'), ('50.459')"
    ],
    snapshot_path = "int32_int64"
);
