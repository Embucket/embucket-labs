{{ config(

    materialized = "incremental",
    unique_key = "dim_ci_build_id",
    full_refresh = only_force_full_refresh(),
    on_schema_change = "sync_all_columns",
    tags=["product"],
    tmp_relation_type = "table"
,
snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user')
]) }},

gitlab_dotcom_ci_builds_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds_source') }}
  {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})

  {% endif %}

),

renamed AS (

  SELECT

    --Surrogate key
    {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_ci_builds_source.ci_build_id']) }} AS dim_ci_build_sk,

    -- LEGACY NATURAL KEY
    gitlab_dotcom_ci_builds_source.ci_build_id                                                                              AS dim_ci_build_id,

    -- NATURAL KEY
    gitlab_dotcom_ci_builds_source.ci_build_id,

    -- FOREIGN KEYS
    gitlab_dotcom_ci_builds_source.ci_build_project_id                                                                      AS dim_project_id,
    prep_project.dim_namespace_id,
    prep_project.ultimate_parent_namespace_id,
    prep_date.date_id                                                                                                       AS created_date_id,
    COALESCE(prep_namespace_plan_hist.dim_plan_id, 34)                                                                      AS dim_plan_id,
    gitlab_dotcom_ci_builds_source.ci_build_runner_id                                                                       AS dim_ci_runner_id,
    prep_user.dim_user_id,
    gitlab_dotcom_ci_builds_source.ci_build_stage_id                                                                        AS dim_ci_stage_id,

    prep_project.namespace_is_internal,
    gitlab_dotcom_ci_builds_source.status                                                                                   AS ci_build_status,
    gitlab_dotcom_ci_builds_source.finished_at,
    gitlab_dotcom_ci_builds_source.created_at,
    gitlab_dotcom_ci_builds_source.updated_at,
    gitlab_dotcom_ci_builds_source.started_at,
    gitlab_dotcom_ci_builds_source.coverage,
    gitlab_dotcom_ci_builds_source.ci_build_commit_id                                                                       AS commit_id,
    gitlab_dotcom_ci_builds_source.ci_build_name,
    gitlab_dotcom_ci_builds_source.options,
    gitlab_dotcom_ci_builds_source.allow_failure,
    gitlab_dotcom_ci_builds_source.stage,
    gitlab_dotcom_ci_builds_source.ci_build_trigger_request_id                                                              AS trigger_request_id,
    gitlab_dotcom_ci_builds_source.stage_idx,
    gitlab_dotcom_ci_builds_source.tag,
    gitlab_dotcom_ci_builds_source.ref,
    gitlab_dotcom_ci_builds_source.type                                                                                     AS ci_build_type,
    gitlab_dotcom_ci_builds_source.description                                                                              AS ci_build_description,
    gitlab_dotcom_ci_builds_source.ci_build_erased_by_id                                                                    AS erased_by_id,
    gitlab_dotcom_ci_builds_source.ci_build_erased_at                                                                       AS erased_at,
    gitlab_dotcom_ci_builds_source.ci_build_artifacts_expire_at                                                             AS artifacts_expire_at,
    gitlab_dotcom_ci_builds_source.environment,
    gitlab_dotcom_ci_builds_source.ci_build_queued_at                                                                       AS queued_at,
    gitlab_dotcom_ci_builds_source.lock_version,
    gitlab_dotcom_ci_builds_source.coverage_regex,
    gitlab_dotcom_ci_builds_source.ci_build_auto_canceled_by_id                                                             AS auto_canceled_by_id,
    gitlab_dotcom_ci_builds_source.retried,
    gitlab_dotcom_ci_builds_source.protected,
    CASE
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1'
        THEN 'script_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '2'
        THEN 'api_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '3'
        THEN 'stuck_or_timeout_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '4'
        THEN 'runner_or_system_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '5'
        THEN 'missing_dependency_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '6'
        THEN 'runner_unsupported'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '7'
        THEN 'stale_schedule'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '8'
        THEN 'job_execution_timeout'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '9'
        THEN 'archived_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '10'
        THEN 'unmet_prerequisites'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '11'
        THEN 'scheduler_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '12'
        THEN 'data_integrity_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '13'
        THEN 'forward_deployment_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '14'
        THEN 'user_blocked'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '15'
        THEN 'project_deleted'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '16'
        THEN 'ci_quota_exceeded'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '17'
        THEN 'pipeline_loop_detected'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '18'
        THEN 'no_matching_runner'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '19'
        THEN 'trace_size_exceeded'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '20'
        THEN 'builds_disabled'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '21'
        THEN 'environment_creation_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '22'
        THEN 'deployment_rejected'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '23'
        THEN 'failed_outdated_deployment_job'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1000'
        THEN 'protected_environment_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1001'
        THEN 'insufficient_bridge_permissions'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1002'
        THEN 'downstream_bridge_project_not_found'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1003'
        THEN 'invalid_bridge_trigger'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1004'
        THEN 'upstream_bridge_project_not_found'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1005'
        THEN 'insufficient_upstream_permissions'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1006'
        THEN 'bridge_pipeline_is_child_pipeline'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1007'
        THEN 'donstream_pipeline_creation_failed'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1008'
        THEN 'secrets_provider_not_found'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1009'
        THEN 'reached_max_descendant_pipelines_depth'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1010'
        THEN 'ip_restriction_failure'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1011'
        THEN 'reached_max_pipeline_hierarchy_size'
      WHEN gitlab_dotcom_ci_builds_source.failure_reason = '1012'
        THEN 'reached_downstream_pipeline_trigger_rate_limit'
      ELSE gitlab_dotcom_ci_builds_source.failure_reason
    END                                                                                                                     AS failure_reason,
    gitlab_dotcom_ci_builds_source.failure_reason                                                                           AS failure_reason_id,
    gitlab_dotcom_ci_builds_source.ci_build_scheduled_at                                                                    AS scheduled_at,
    gitlab_dotcom_ci_builds_source.upstream_pipeline_id,
    CASE
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%apifuzzer_fuzz%'
        THEN 'api_fuzzing'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%container_scanning%'
        THEN 'container_scanning'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%dast%'
        THEN 'dast'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%dependency_scanning%'
        THEN 'dependency_scanning'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%license_management%'
        THEN 'license_management'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%license_scanning%'
        THEN 'license_scanning'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%sast%'
        THEN 'sast'
      WHEN gitlab_dotcom_ci_builds_source.ci_build_name LIKE '%secret_detection%'
        THEN 'secret_detection'
    END                                                                                                                     AS secure_ci_build_type,
    gitlab_dotcom_ci_builds_source.scheduling_type                                                                          AS scheduling_type

  FROM gitlab_dotcom_ci_builds_source
  LEFT JOIN prep_project
    ON gitlab_dotcom_ci_builds_source.ci_build_project_id = prep_project.dim_project_id
  LEFT JOIN prep_namespace_plan_hist
    ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_ci_builds_source.created_at >= prep_namespace_plan_hist.valid_from
      AND gitlab_dotcom_ci_builds_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
  LEFT JOIN prep_user
    ON gitlab_dotcom_ci_builds_source.ci_build_user_id = prep_user.dim_user_id
  LEFT JOIN prep_date
    ON TO_DATE(gitlab_dotcom_ci_builds_source.created_at) = prep_date.date_day
  WHERE gitlab_dotcom_ci_builds_source.ci_build_project_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@nhervas",
    created_date="2021-06-17",
    updated_date="2024-08-01"
) }}
