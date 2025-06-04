{{ config(
    materialized='incremental',
    unique_key= 'project_statistics_snapshot_pk',
    on_schema_change='sync_all_columns'
  ) 
}}

{% if is_incremental() %}
  {% set max_snapshot_date_query %}
    SELECT MAX(snapshot_day) FROM {{this}}
  {% endset %}
  
  {% set results = run_query(max_snapshot_date_query) %}
  
  {% if execute %}
    {% set max_snapshot_date = results.columns[0][0] %}
  {% else %}
    {% set max_snapshot_date = [] %}
  {% endif %}

{% endif %}

WITH gitlab_dotcom_project_statistics_snapshots AS (

  SELECT*
  FROM {{ ref('gitlab_dotcom_project_statistics_snapshots_base') }}
  {% if is_incremental() %}
   WHERE  COALESCE(valid_to, CURRENT_DATE) >= '{{ max_snapshot_date }}'
  {% endif %}

), gitlab_dotcom_namespace_lineage_historical_daily AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_daily') }}
  {% if is_incremental() %}
   WHERE snapshot_day >= '{{ max_snapshot_date }}'
  {% endif %}

), date_details AS (

    SELECT *
    FROM {{ ref("dim_date") }}
    -- reduce size of results significantly
    WHERE date_actual > '2020-03-01'
      AND date_actual <  {{ dbt.current_timestamp() }}::DATE
    {% if is_incremental() %}
      AND date_actual >= '{{ max_snapshot_date }}'
    {% endif %}

), project_snapshots AS (

   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM gitlab_dotcom_project_statistics_snapshots
   QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id, valid_from::DATE ORDER BY valid_from DESC) = 1

), project_snapshots_daily AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['date_details.date_actual', 'project_snapshots.project_statistics_id']) }} AS project_statistics_snapshot_pk,
      date_details.date_actual                                                                                        AS snapshot_day,
      project_snapshots.project_statistics_id,
      project_snapshots.project_id,
      project_snapshots.namespace_id,
      gitlab_dotcom_namespace_lineage_historical_daily.ultimate_parent_id                                             AS ultimate_parent_namespace_id,
      gitlab_dotcom_namespace_lineage_historical_daily.ultimate_parent_plan_id,
      gitlab_dotcom_namespace_lineage_historical_daily.finance_pl_category,
      project_snapshots.commit_count,
      project_snapshots.storage_size,
      project_snapshots.repository_size,
      project_snapshots.container_registry_size,
      project_snapshots.lfs_objects_size,
      project_snapshots.build_artifacts_size,
      project_snapshots.packages_size,
      project_snapshots.wiki_size,
      project_snapshots.shared_runners_seconds,
      project_snapshots.last_update_started_at,
      COALESCE(project_snapshots.build_artifacts_size, 0) / POW(1024, 3)                                              AS build_artifacts_gb,
      COALESCE(project_snapshots.repository_size, 0) / POW(1024, 3)                                                   AS repo_size_gb,
      COALESCE(project_snapshots.container_registry_size, 0) / POW(1024, 3)                                           AS container_registry_gb
    FROM project_snapshots
    INNER JOIN date_details
      ON date_details.date_actual >= project_snapshots.valid_from::DATE
      AND date_details.date_actual < project_snapshots.valid_to_::DATE
    LEFT JOIN gitlab_dotcom_namespace_lineage_historical_daily
      ON date_details.date_actual = gitlab_dotcom_namespace_lineage_historical_daily.snapshot_day
        AND project_snapshots.namespace_id = gitlab_dotcom_namespace_lineage_historical_daily.namespace_id

)

SELECT *
FROM project_snapshots_daily
