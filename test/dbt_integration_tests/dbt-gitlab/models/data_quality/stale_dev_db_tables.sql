

{{ simple_cte([
    ('snowflake_databases', 'snowflake_databases_source'),
    ('snowflake_tables', 'snowflake_tables_source'),
    ('snowflake_users', 'snowflake_show_users_source'),
    ('dim_team_member', 'dim_team_member'),
]) }},

dev_databases AS (

  SELECT
    database_id,
    database_name,
    database_owner
  FROM snowflake_databases
  WHERE database_owner NOT IN (
      'TRANSFORMER',
      'ACCOUNTADMIN',
      'SYSADMIN',
      'LOADER',
      'STATIC_ADMIN'
    )
    AND database_name LIKE '%' || snowflake_databases.database_owner || '_%'

),

stale_dev_db_tables AS (

  SELECT
    snowflake_tables.table_name,
    snowflake_tables.table_schema,
    snowflake_tables.table_catalog,
    snowflake_tables.table_type,
    snowflake_tables.created,
    snowflake_tables.last_altered,
    snowflake_tables.table_owner,
    dev_databases.database_owner
  FROM snowflake_tables
  INNER JOIN dev_databases ON snowflake_tables.table_catalog_id = dev_databases.database_id
  WHERE snowflake_tables.deleted IS NULL
    AND table_schema != 'INFORMATION_SCHEMA'
    AND snowflake_tables.table_type = 'BASE TABLE'
    AND snowflake_tables.last_altered <= DATEADD(D, {{ var('dev_db_object_expiration') }}, CURRENT_TIMESTAMP())
  ORDER BY dev_databases.database_owner

),

snowflake_dev_users AS (

  SELECT
    user_name,
    email
  FROM snowflake_users
  WHERE is_disabled = FALSE
    AND user_name IN (
      SELECT DISTINCT database_owner
      FROM stale_dev_db_tables
    )

),

gitlab_team_members AS (

  SELECT
    work_email,
    gitlab_username
  FROM MY_SNOWFLAKE_PREP_DATABASE.common.dim_team_member
  WHERE is_current = TRUE
    AND is_current_team_member = TRUE

),

final AS (

  SELECT DISTINCT
    stale_dev_db_tables.table_name,
    stale_dev_db_tables.table_schema,
    stale_dev_db_tables.table_catalog,
    stale_dev_db_tables.created,
    stale_dev_db_tables.last_altered,
    stale_dev_db_tables.database_owner,
    gitlab_team_members.gitlab_username
  FROM stale_dev_db_tables
  LEFT JOIN snowflake_dev_users ON LOWER(stale_dev_db_tables.database_owner) = LOWER(snowflake_dev_users.user_name)
  LEFT JOIN gitlab_team_members ON LOWER(gitlab_team_members.work_email) = LOWER(snowflake_dev_users.email)

)


SELECT *
FROM final
ORDER BY gitlab_username
