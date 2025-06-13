{{ config(
    materialized='incremental',
    unique_key= 'project_statistics_snapshot_pk',
    on_schema_change='sync_all_columns'
  ) 
}}

WITH project_snapshots_daily AS (

    SELECT
      prep_gitlab_dotcom_project_statistics_daily_snapshot.project_statistics_snapshot_pk,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.project_statistics_id,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.project_id,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.namespace_id,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.ultimate_parent_namespace_id,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.ultimate_parent_plan_id,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.commit_count,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.storage_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.repository_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.container_registry_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.lfs_objects_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.build_artifacts_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.packages_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.wiki_size,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.shared_runners_seconds,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.last_update_started_at,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.build_artifacts_gb,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb,
      prep_gitlab_dotcom_project_statistics_daily_snapshot.container_registry_gb
    FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
    {% if is_incremental() %}
    WHERE snapshot_day >= (SELECT MAX(snapshot_day) FROM {{ this }})
    {% endif %}

)

SELECT *
FROM project_snapshots_daily
