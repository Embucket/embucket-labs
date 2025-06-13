{{
  config(
    materialized='table'
  )
}}

WITH source AS (
  SELECT *
  FROM {{ ref('snowflake_queries') }}
),

resource_efficiency_fct AS (
  -- columns left in for future fct table.
  SELECT
    -- invocation
    dbt_invocation_id,
    user_name,
    role_name,
    user_type,
    division,
    department,
    dbt_version,
    dbt_run_started_at::TIMESTAMP                  AS dbt_run_started_at,
    dbt_runner,
    is_invocation_full_refresh,
    runner_source,
    airflow_dag_id,
    airflow_task_id,
    airflow_dag_run_date_id,
    airflow_run_id,
    airflow_try_number,
    airflow_orchestration,
    ci_user_id,
    ci_merge_request_id,
    ci_build_id,

    -- model
    MD5(dbt_invocation_id || '-' || resource_id)   AS resource_execution_id,
    warehouse_name,
    warehouse_id,
    is_model_full_refresh,
    model_materialization,
    resource_file,
    resource_id,
    resource_name,
    resource_type,
    package_name,
    relation_database,
    relation_schema,
    relation_identifier,

    -- query    
    IFF(query_type IN (
      'CREATE_VIEW',
      'INSERT',
      'DELETE',
      'CREATE_TABLE_AS_SELECT',
      'MERGE',
      'CREATE_VIEW',
      'SELECT',
      'EXTERNAL_TABLE_REFRESH'
    ),
    'action', 'admin')                             AS query_category,
    ARRAY_AGG(DISTINCT query_type)                 AS resource_query_types,
    ARRAY_AGG(DISTINCT execution_status)           AS resource_execution_status,
    ARRAY_AGG(DISTINCT cluster_number)             AS resource_clusters,
    ARRAY_AGG(DISTINCT query_id)                   AS resource_queries,
    SUM(bytes_scanned)                             AS resource_bytes_scanned,
    SUM(bytes_written)                             AS resource_bytes_written,
    SUM(bytes_spilled_to_remote_storage)           AS resource_bytes_spilled_to_remote_storage,
    SUM(bytes_spilled_to_local_storage)            AS resource_bytes_spilled_to_local_storage,
    SUM(partitions_total)                          AS resource_partitions_total,
    SUM(partitions_scanned)                        AS resource_partitions_scanned,
    COUNT(query_id)                                AS resource_query_count,
    GREATEST((
      resource_bytes_scanned
      - (resource_bytes_spilled_to_remote_storage)
    )
    / NULLIFZERO(resource_bytes_scanned), 0) AS resource_remote_storage_efficiency,
    GREATEST((
      resource_bytes_scanned
      - (resource_bytes_spilled_to_local_storage)
    )
    / NULLIFZERO(resource_bytes_scanned), 0) AS resource_local_storage_efficiency,
    GREATEST(CASE WHEN resource_partitions_total = 1 THEN 1
      ELSE (
        resource_partitions_total
        - resource_partitions_scanned
      )
      / NULLIFZERO(resource_partitions_total)
    END, 0)                                        AS resource_partition_efficiency,
    GREATEST((
      (resource_local_storage_efficiency * 0.25)
      + (resource_remote_storage_efficiency * 0.5)
      + (resource_partition_efficiency * 0.25)
    ) * 100, 0)                                    AS resource_efficiency_score
  FROM source
  WHERE dbt_metadata IS NOT NULL
  {{ dbt_utils.group_by(n=34) }}


),

report_filters AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['resource_id','DATE_TRUNC(day,dbt_run_started_at)']) }} AS report_key, 
    dbt_invocation_id,
    dbt_version,
    dbt_run_started_at,
    is_invocation_full_refresh,
    airflow_dag_id,
    airflow_task_id,
    airflow_dag_run_date_id,
    airflow_run_id,
    airflow_try_number,
    airflow_orchestration,
    is_model_full_refresh,
    model_materialization,
    resource_id,
    resource_name,
    resource_type,
    resource_bytes_scanned,
    resource_bytes_written,
    resource_bytes_spilled_to_remote_storage,
    resource_bytes_spilled_to_local_storage,
    resource_partitions_total,
    resource_partitions_scanned,
    resource_query_count,
    resource_remote_storage_efficiency,
    resource_local_storage_efficiency,
    resource_partition_efficiency,
    resource_efficiency_score
  FROM resource_efficiency_fct
  WHERE TRUE
    AND ARRAY_SIZE(resource_execution_status) = 1
    AND ARRAY_CONTAINS('SUCCESS'::VARIANT, resource_execution_status) = TRUE
    AND query_category = 'action'
    AND model_materialization != 'view'
    AND resource_type = 'model'
    AND user_name = 'AIRFLOW'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', dbt_run_started_at), relation_identifier ORDER BY dbt_run_started_at DESC) = 1
    AND dbt_run_started_at < DATE_TRUNC('day',MAX(dbt_run_started_at) OVER ())
),

report AS (
  SELECT
    *,
    SUM(resource_bytes_written) OVER (PARTITION BY DATE_TRUNC('day', dbt_run_started_at)) AS dbt_day_bytes_written
  FROM report_filters
)

SELECT *
FROM report
