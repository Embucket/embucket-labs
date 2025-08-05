use crate::test_query;

test_query!(
    drop_database_error_in_use,
    "DROP DATABASE embucket",
    snapshot_path = "database"
);

test_query!(
    drop_database,
    "DROP DATABASE embucket",
    setup_queries = ["DROP SCHEMA embucket.public"],
    snapshot_path = "database"
);

test_query!(
    create_database,
    "SHOW DATABASES STARTS WITH 'db_test'",
    setup_queries = ["CREATE DATABASE db_test external_volume = 'test_volume'"],
    snapshot_path = "database"
);

test_query!(
    create_database_with_new_volume,
    "SHOW DATABASES STARTS WITH 'db_test'",
    setup_queries = [
        "CREATE EXTERNAL VOLUME mem STORAGE_LOCATIONS = ((NAME = 'mem_vol' STORAGE_PROVIDER = 'MEMORY'))",
        "CREATE DATABASE db_test external_volume = 'mem'"
    ],
    snapshot_path = "database"
);
