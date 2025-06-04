{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('gitlab_dotcom_ci_pipelines_source', 'gitlab_dotcom_ci_pipelines_source'),
    ('prep_date', 'prep_date'),
    ('pipeline_metadata_internal_only', 'gitlab_dotcom_ci_pipeline_metadata_internal_only_source')
]) }},

renamed AS (

  SELECT

    -- SURROGATE KEY
    {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_ci_pipelines_source.ci_pipeline_id']) }}                                  AS dim_ci_pipeline_sk,

    -- NATURAL KEY
    gitlab_dotcom_ci_pipelines_source.ci_pipeline_id,

    -- LEGACY NATURAL KEY
    gitlab_dotcom_ci_pipelines_source.ci_pipeline_id                                            AS dim_ci_pipeline_id,

    -- FOREIGN KEYS
    gitlab_dotcom_ci_pipelines_source.project_id                                                AS dim_project_id,
    prep_project.dim_namespace_id,
    prep_project.ultimate_parent_namespace_id,
    prep_user.dim_user_id,
    prep_date.date_id                                                                           AS created_date_id,
    COALESCE(prep_namespace_plan_hist.dim_plan_id, 34)                                          AS dim_plan_id,
    merge_request_id,

    gitlab_dotcom_ci_pipelines_source.created_at,
    gitlab_dotcom_ci_pipelines_source.started_at,
    gitlab_dotcom_ci_pipelines_source.updated_at,
    gitlab_dotcom_ci_pipelines_source.committed_at,
    gitlab_dotcom_ci_pipelines_source.finished_at,
    gitlab_dotcom_ci_pipelines_source.ci_pipeline_duration                                      AS ci_pipeline_duration_in_s,

    gitlab_dotcom_ci_pipelines_source.status,
    gitlab_dotcom_ci_pipelines_source.ref,
    gitlab_dotcom_ci_pipelines_source.has_tag,
    gitlab_dotcom_ci_pipelines_source.yaml_errors,
    gitlab_dotcom_ci_pipelines_source.lock_version,
    gitlab_dotcom_ci_pipelines_source.auto_canceled_by_id,
    gitlab_dotcom_ci_pipelines_source.pipeline_schedule_id,
    CASE
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 1 THEN 'push'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 2 THEN 'web'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 3 THEN 'trigger'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 4 THEN 'schedule'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 5 THEN 'api'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 6 THEN 'external'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 7 THEN 'pipeline'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 8 THEN 'chat'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 9 THEN 'webide'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 10 THEN 'merge_request_event'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 11 THEN 'external_pull_request_event'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 12 THEN 'parent_pipeline'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 13 THEN 'ondemand_dast_scan'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 14 THEN 'ondemande_dast_validation'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 15 THEN 'security_orchestration_policy'
      WHEN gitlab_dotcom_ci_pipelines_source.ci_pipeline_source = 16 THEN 'container_registry_push'
    END                                                                                         AS ci_pipeline_source,
    gitlab_dotcom_ci_pipelines_source.ci_pipeline_source                                        AS ci_pipeline_source_id,
    gitlab_dotcom_ci_pipelines_source.config_source,
    gitlab_dotcom_ci_pipelines_source.is_protected,
    gitlab_dotcom_ci_pipelines_source.failure_reason                                            AS failure_reason_id,
    {{ map_ci_pipeline_failure_reason('failure_reason_id') }}                                   AS failure_reason,
    gitlab_dotcom_ci_pipelines_source.ci_pipeline_iid                                           AS ci_pipeline_internal_id,
    gitlab_dotcom_ci_pipelines_source.is_deleted,
    gitlab_dotcom_ci_pipelines_source.is_deleted_updated_at,
    pipeline_metadata_internal_only.name                                                        AS pipeline_name_internal_only
  FROM gitlab_dotcom_ci_pipelines_source
  LEFT JOIN prep_project
    ON gitlab_dotcom_ci_pipelines_source.project_id = prep_project.dim_project_id
  LEFT JOIN prep_namespace_plan_hist
    ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_ci_pipelines_source.created_at >= prep_namespace_plan_hist.valid_from
      AND gitlab_dotcom_ci_pipelines_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
  LEFT JOIN prep_user
    ON gitlab_dotcom_ci_pipelines_source.user_id = prep_user.dim_user_id
  LEFT JOIN prep_date
    ON TO_DATE(gitlab_dotcom_ci_pipelines_source.created_at) = prep_date.date_day
  LEFT JOIN pipeline_metadata_internal_only
    ON pipeline_metadata_internal_only.ci_pipeline_id = gitlab_dotcom_ci_pipelines_source.ci_pipeline_id
  WHERE gitlab_dotcom_ci_pipelines_source.project_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-06-10",
    updated_date="2025-01-14"
) }}
