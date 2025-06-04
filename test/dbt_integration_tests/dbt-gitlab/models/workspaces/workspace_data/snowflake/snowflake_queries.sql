{{
  config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH source AS (

  SELECT *
  FROM {{ ref('snowflake_query_history_source') }}
  {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE query_end_at > (SELECT MAX(query_end_at) FROM {{ this }})

  {% endif %}

),

query_metering AS (

  SELECT *
  FROM {{ ref('snowflake_query_metering') }}
),

user_map AS (
  SELECT *
  FROM {{ ref('map_team_member_snowflake_user') }}
),

team_members AS (
  SELECT *
  FROM {{ ref('employee_directory_analysis') }}
),

user_types AS (
  SELECT *
  FROM {{ ref('snowflake_non_team_member_user_type_source') }}
),

credit_rates AS (
  SELECT 
    contract_rate_effective_date AS contract_rate_effective_start_date,
    DATEADD(day, -1, LEAD(contract_rate_effective_date, 1, {{ var('tomorrow') }}) 
      OVER ( ORDER BY contract_rate_effective_date ASC )) AS contract_rate_effective_end_date,
    contract_rate
  FROM {{ ref('snowflake_contract_rates_source') }}
),

expanded AS (

  SELECT
    source.database_name,
    source.database_id,
    source.query_id,
    source.query_type,
    source.query_text,
    source.query_tag,
    source.execution_status,
    source.schema_name,
    source.schema_id,
    source.user_name,
    source.role_name,
    source.warehouse_name,
    source.warehouse_id,
    source.warehouse_size,
    source.warehouse_type,
    source.cluster_number,
    source.query_start_at,
    source.query_end_at,
    source.compilation_time,
    source.execution_time,
    source.total_elapsed_time,
    source.queued_provisioning_time,
    source.queued_repair_time,
    source.queued_overload_time,
    source.transaction_blocked_time,
    source.rows_produced,
    source.bytes_scanned,
    source.bytes_written,
    source.bytes_spilled_to_remote_storage,
    source.bytes_spilled_to_local_storage,
    source.credits_used_cloud_services,
    source.percentage_scanned_from_cache,
    source.partitions_total,
    source.partitions_scanned,
    query_metering.total_attributed_credits,
    (credit_rates.contract_rate * query_metering.total_attributed_credits)::NUMBER(38,2) AS dollars_spent,
    IFF(team_members.employee_id IS NULL, user_types.user_type, 'Team Member')           AS user_type,
    IFF(user_types.user_type IS NULL, team_members.division, user_types.division)        AS division,
    IFF(user_types.user_type IS NULL, team_members.department, user_types.department)    AS department,
    TRY_PARSE_JSON(REGEXP_SUBSTR(source.query_text, '\{\"app\".*\}'))                    AS dbt_metadata,
    dbt_metadata['dbt_version']::VARCHAR                                                 AS dbt_version,
    dbt_metadata['profile_name']::VARCHAR                                                AS dbt_profile_name,
    dbt_metadata['target_name']::VARCHAR                                                 AS dbt_target_name,
    dbt_metadata['target_user']::VARCHAR                                                 AS dbt_target_user,
    dbt_metadata['invocation_id']::VARCHAR                                               AS dbt_invocation_id,
    dbt_metadata['run_started_at']::VARCHAR                                              AS dbt_run_started_at,
    dbt_metadata['full_refresh']::BOOLEAN                                                AS is_model_full_refresh,
    dbt_metadata['is_full_refresh']::BOOLEAN                                             AS is_invocation_full_refresh,
    dbt_metadata['materialized']::VARCHAR                                                AS model_materialization,
    dbt_metadata['runner']::VARCHAR                                                      AS dbt_runner,
    dbt_metadata['file']::VARCHAR                                                        AS resource_file,
    dbt_metadata['node_id']::VARCHAR                                                     AS resource_id,
    dbt_metadata['node_name']::VARCHAR                                                   AS resource_name,
    dbt_metadata['resource_type']::VARCHAR                                               AS resource_type,
    dbt_metadata['package_name']::VARCHAR                                                AS package_name,
    dbt_metadata['relation']['database']::VARCHAR                                        AS relation_database,
    dbt_metadata['relation']['schema']::VARCHAR                                          AS relation_schema,
    dbt_metadata['relation']['identifier']::VARCHAR                                      AS relation_identifier,
    CASE
      WHEN rlike(dbt_runner,'[0-9]+-[0-9]+-?[0-9]*$') THEN 'ci'
      WHEN rlike(dbt_runner,'.+__.+__.+__.+__.+__.+$') THEN 'airflow'
    ELSE dbt_runner
    END AS runner_source,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 1),NULL) AS airflow_dag_id,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 2),NULL) AS airflow_task_id,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 3),NULL) AS airflow_dag_run_date_id,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 4),NULL) || '__' || SPLIT_PART(dbt_runner, '__', 5) AS airflow_run_id,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 6),NULL) AS airflow_try_number,
    IFF(runner_source = 'airflow',SPLIT_PART(dbt_runner, '__', 4),NULL) AS airflow_orchestration,
    IFF(runner_source = 'ci',TRY_TO_NUMBER(SPLIT_PART(dbt_runner,'-',1)),NULL) AS ci_user_id,
    IFF(runner_source = 'ci',TRY_TO_NUMBER(SPLIT_PART(dbt_runner,'-',2)),NULL) AS ci_merge_request_id,
    IFF(runner_source = 'ci',TRY_TO_NUMBER(SPLIT_PART(dbt_runner,'-',3)),NULL) AS ci_build_id,
    TRY_PARSE_JSON(REGEXP_SUBSTR(query_tag, '\{ \"tableau-query-origins\".*\}'))   AS tableau_metadata,
    tableau_metadata['tableau-query-origins']['dashboard-luid']::VARCHAR AS tableau_dashboard_luid,
    tableau_metadata['tableau-query-origins']['site-luid']::VARCHAR AS tableau_site_luid,
    tableau_metadata['tableau-query-origins']['user-luid']::VARCHAR AS tableau_user_luid,
    tableau_metadata['tableau-query-origins']['workbook-luid']::VARCHAR AS tableau_workbook_luid,
    NULLIF(tableau_metadata['tableau-query-origins']['worksheet-luid']::VARCHAR,'') AS tableau_worksheet_luid
  FROM source
  LEFT JOIN query_metering
    ON source.query_id = query_metering.query_id
  LEFT JOIN credit_rates
    ON source.query_start_at BETWEEN credit_rates.contract_rate_effective_start_date AND credit_rates.contract_rate_effective_end_date
  LEFT JOIN user_types
    ON user_types.user_name = source.user_name
  LEFT JOIN user_map
    ON source.user_name = user_map.snowflake_user_name
  LEFT JOIN team_members
    ON user_map.employee_id = team_members.employee_id
      AND DATE_TRUNC('day', source.query_start_at) = team_members.date_actual

)

SELECT *
FROM expanded
