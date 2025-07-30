use crate::test_query;

test_query!(
    create_schema,
    "SHOW SCHEMAS IN embucket STARTS WITH 'new'",
    setup_queries = [
        "CREATE SCHEMA embucket.new_schema",
        "CREATE SCHEMA embucket.\"new schema\""
    ],
    snapshot_path = "schema"
);

test_query!(
    drop_schema_quoted_identifiers,
    "SHOW SCHEMAS IN embucket STARTS WITH 'test'",
    setup_queries = [
        "CREATE SCHEMA embucket.\"test public\"",
        "DROP SCHEMA embucket.\"test public\"",
    ],
    snapshot_path = "schema"
);
