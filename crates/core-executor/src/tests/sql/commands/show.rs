use crate::test_query;

// SHOW DATABASES
test_query!(
    show_databases,
    "SHOW DATABASES",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_databases_filter,
    "SHOW DATABASES STARTS WITH 'em'",
    snapshot_path = "show"
);

// SHOW SCHEMAS
test_query!(
    show_schemas,
    "SHOW SCHEMAS",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_schemas_starts_with,
    "SHOW SCHEMAS STARTS WITH 'publ'",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_schemas_in_db,
    "SHOW SCHEMAS IN embucket",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_schemas_in_db_and_prefix,
    "SHOW SCHEMAS IN embucket STARTS WITH 'pub'",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_schemas_in_missing_db,
    "SHOW SCHEMAS IN test",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_schemas_in_missing_db_with_in_key,
    "SHOW SCHEMAS IN DATABASE test",
    sort_all = true,
    snapshot_path = "show"
);

// SHOW TABLES
test_query!(
    show_tables,
    "SHOW TABLES",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_in_database,
    "SHOW TABLES IN DATABASE embucket",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_starts_with,
    "SHOW TABLES STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_in_schema,
    "SHOW TABLES IN public",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_in_missing_schema,
    "SHOW TABLES IN test_schema",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_in_schema_full,
    "SHOW TABLES IN embucket.public",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_tables_in_schema_and_prefix,
    "SHOW TABLES IN public STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "show"
);
// context name injection
test_query!(
    show_tables_context_name_injection,
    "SHOW TABLES IN new_schema",
    setup_queries = [
        "CREATE SCHEMA embucket.new_schema",
        "SET schema = 'new_schema'",
        "CREATE table new_table (id INT)",
    ],
    snapshot_path = "show"
);

// SHOW VIEWS
test_query!(
    show_views,
    "SHOW VIEWS",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_views_starts_with,
    "SHOW VIEWS STARTS WITH 'schem'",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_views_in_schema,
    "SHOW VIEWS IN information_schema",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_views_in_schema_full,
    "SHOW VIEWS IN embucket.information_schema",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_views_in_schema_and_prefix,
    "SHOW VIEWS IN information_schema STARTS WITH 'schem'",
    sort_all = true,
    snapshot_path = "show"
);

// SHOW COLUMNS
test_query!(
    show_columns,
    "SHOW COLUMNS",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_columns_in_table,
    "SHOW COLUMNS IN employee_table",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_columns_in_missing_table,
    "SHOW COLUMNS IN missing",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_columns_in_table_full,
    "SHOW COLUMNS IN embucket.public.employee_table",
    sort_all = true,
    snapshot_path = "show"
);

test_query!(
    show_columns_starts_with,
    "SHOW COLUMNS IN employee_table STARTS WITH 'last_'",
    sort_all = true,
    snapshot_path = "show"
);

// SHOW OBJECTS
test_query!(
    show_objects,
    "SHOW OBJECTS",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_objects_starts_with,
    "SHOW OBJECTS STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_objects_in_database,
    "SHOW OBJECTS IN DATABASE embucket",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_objects_in_schema,
    "SHOW OBJECTS IN public",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_objects_in_schema_full,
    "SHOW OBJECTS IN embucket.public",
    sort_all = true,
    snapshot_path = "show"
);
test_query!(
    show_objects_in_schema_and_prefix,
    "SHOW OBJECTS IN public STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "show"
);

// TODO SHOW PARAMETERS is not supported yet
test_query!(
    show_parameters,
    "SHOW PARAMETERS",
    sort_all = true,
    snapshot_path = "show"
);

test_query!(
    use_role,
    "SHOW VARIABLES",
    setup_queries = ["USE ROLE test_role"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);

test_query!(
    use_secondary_roles,
    "SHOW VARIABLES",
    setup_queries = ["USE SECONDARY ROLES test_role"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    use_warehouse,
    "SHOW VARIABLES",
    setup_queries = ["USE WAREHOUSE test_warehouse"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    use_database,
    "SHOW VARIABLES",
    setup_queries = ["USE DATABASE test_db"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    use_schema,
    "SHOW VARIABLES",
    setup_queries = ["USE SCHEMA test_schema"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    show_variables_multiple,
    "SHOW VARIABLES",
    setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    sort_all = true,
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    set_variable,
    "SHOW VARIABLES",
    setup_queries = ["SET v1 = 'test'"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
test_query!(
    set_variable_subquery,
    "SHOW VARIABLES",
    setup_queries = ["SET id_threshold = (
            SELECT COUNT(*) * 100 FROM (
                SELECT 1 AS column1 UNION ALL SELECT 2 UNION ALL SELECT 3
            ) AS table1
        );"],
    exclude_columns = ["created_on", "updated_on", "session_id"],
    snapshot_path = "show"
);
