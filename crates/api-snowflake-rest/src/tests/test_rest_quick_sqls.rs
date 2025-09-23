#[cfg(feature = "default-server")]
use crate::server::test_server::run_test_rest_api_server;

// External server should be already running, we just return its address
#[cfg(not(feature = "default-server"))]
use crate::tests::external_server::run_test_rest_api_server;

use crate::sql_test;

// This test uses external server if tests were executed with `cargo test-rest`
// Or on CI/CD and in all other cases it creates its own server for every test.
// Tests executed sequentially, as of:
//  - not enough granularity for query_id, so parallel executions would lead to ids clashes;
//  - no queues of queries yet, so parallel execution would exceeded concurrency limit;
//  - it uses external server so predictability of parallel execution would be an issue too.

mod snowflake_compatibility {
    use super::*;

    sql_test!(
        create_table_bad_syntax,
        [
            // "Snowflake:
            // 001003 (42000): UUID: SQL compilation error:
            // syntax error line 1 at position 16 unexpected '<EOF>'."
            "create table foo",
        ]
    );

    sql_test!(
        create_table_missing_schema,
        [
            // "Snowflake:
            // 002003 (02000): SQL compilation error:
            // Schema 'TESTS.MISSING_SCHEMA' does not exist or not authorized."
            "create table missing_schema.foo(a int)",
        ]
    );

    sql_test!(
        show_schemas_in_missing_db,
        [
            // "Snowflake:
            // 002043 (02000): UUID: SQL compilation error:
            // Object does not exist, or operation cannot be performed."
            "show schemas in database missing_db",
        ]
    );

    sql_test!(
        select_1,
        [
            // "Snowflake:
            // +---+
            // | 1 |
            // |---|
            // | 1 |
            // +---+"
            "select 1",
        ]
    );

    sql_test!(
        select_1_async,
        [
            // scheduled query ID
            "select 1;>",
            // +---+
            // | 1 |
            // |---|
            // | 1 |
            // +---+"
            "!result $LAST_QUERY_ID",
        ]
    );

    // This test uses non standard "sleep" function, so it should not be executed against Snowflake
    // In Snowflake kind of equivalent is stored procedure: "CALL SYSTEM$WAIT(1);"
    sql_test!(
        async_sleep_result,
        [
            // scheduled query ID
            "select sleep(1);>",
            // +-----------------+
            // | sleep(Int64(1)) |
            // |-----------------|
            // | 1               |
            // +-----------------+
            "!result $LAST_QUERY_ID",
        ]
    );

    sql_test!(
        cancel_query_bad_id1,
        [
            // Invalid UUID.
            "SELECT SYSTEM$CANCEL_QUERY(1);",
        ]
    );

    sql_test!(
        cancel_query_bad_id2,
        [
            // Invalid UUID.
            "SELECT SYSTEM$CANCEL_QUERY('1');",
        ]
    );

    sql_test!(
        cancel_query_not_running,
        [
            // Invalid UUID.
            "SELECT SYSTEM$CANCEL_QUERY('5a5f2c6c-8aee-4c18-8285-273fa60e44ae');",
        ]
    );

    sql_test!(
        abort_query_bad_id,
        [
            // Invalid UUID.
            "!abort 1",
        ]
    );

    sql_test!(
        abort_ok_query,
        [
            // 1: scheduled query ID
            "SELECT sleep(1);>",
            // 2: query [UUID] terminated.
            "!abort $LAST_QUERY_ID",
        ]
    );

    sql_test!(
        cancel_ok_query,
        [
            // 1: scheduled query ID
            "SELECT sleep(1);>",
            // 2: query [UUID] terminated.
            "SELECT SYSTEM$CANCEL_QUERY('$LAST_QUERY_ID');",
        ]
    );

    sql_test!(
        cancel_ok_sleeping_query,
        [
            // 1: scheduled query ID
            "SELECT SLEEP(1);>",
            // 2: query [UUID] terminated.
            "SELECT SYSTEM$CANCEL_QUERY('$LAST_QUERY_ID');",
        ]
    );
}

// Following tests so far
mod snowflake_compatibility_issues {
    use super::*;

    // incorrect sql_state: 42000, should be: 42S02
    sql_test!(
        select_from_missing_table,
        [
            // "Snowflake:
            // 002003 (42S02): SQL compilation error
            // "Embucket:
            // 002003 (02000): SQL compilation error
            "select * from missing_table",
        ]
    );

    // incorrect message
    sql_test!(
        select_from_missing_schema,
        [
            // "Snowflake:
            // 002003 (02000): SQL compilation error:
            // Schema 'TESTS.MISSING_SCHEMA' does not exist or not authorized.
            // "Embucket:
            // 002003 (02000): SQL compilation error:
            // table 'embucket.missing_schema.foo' not found
            "select * from missing_schema.foo",
        ]
    );

    // incorrect message
    sql_test!(
        select_from_missing_db,
        [
            // "Snowflake:
            // 002003 (02000): SQL compilation error:
            // Schema 'TESTS.MISSING_SCHEMA' does not exist or not authorized.
            // "Embucket:
            // 002003 (02000): SQL compilation error:
            // table 'embucket.missing_schema.foo' not found
            "select * from missing_db.foo.foo",
        ]
    );
}
