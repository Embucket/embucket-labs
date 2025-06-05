

WITH project_ci_cd_settings AS (

  SELECT *
  FROM EMBUCKET.tap_postgres.gitlab_db_project_ci_cd_settings

),



source AS (

  SELECT
    *,
    TO_TIMESTAMP(_uploaded_at::INT)                                         AS uploaded_at,
    md5(cast(coalesce(cast(project_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(group_runners_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(merge_pipelines_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(default_git_depth as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(forward_deployment_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(merge_trains_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(auto_rollback_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(keep_latest_artifact as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(restrict_user_defined_variables as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(job_token_scope_enabled as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(runner_token_expiration_interval as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(separated_caches as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(allow_fork_pipelines_to_run_in_parent_project as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(inbound_job_token_scope_enabled as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))                                                                      AS record_checksum
  FROM project_ci_cd_settings
  
  

),

base AS (

  SELECT
    *,
    LEAD(_uploaded_at) OVER (PARTITION BY id ORDER BY _uploaded_at)          AS next_uploaded_at,
    LAG(record_checksum, 1, '') OVER (PARTITION BY id ORDER BY _uploaded_at) AS lag_checksum,
    CONDITIONAL_TRUE_EVENT(record_checksum != lag_checksum)
      OVER (PARTITION BY id ORDER BY _uploaded_at)                           AS checksum_group
  FROM source

),

grouped AS (

  SELECT
    id                                   AS project_ci_cd_settings_snapshot_id,
    project_id,
    group_runners_enabled,
    merge_pipelines_enabled,
    default_git_depth,
    forward_deployment_enabled,
    merge_trains_enabled,
    auto_rollback_enabled,
    keep_latest_artifact,
    restrict_user_defined_variables,
    job_token_scope_enabled,
    runner_token_expiration_interval,
    separated_caches,
    allow_fork_pipelines_to_run_in_parent_project,
    inbound_job_token_scope_enabled,
    MIN(uploaded_at)                        AS uploaded_at,
    TO_TIMESTAMP(MIN(_uploaded_at)::INT)    AS valid_from,
    IFF(
      MAX(COALESCE(next_uploaded_at, 9999999999) = 9999999999),
      NULL, TO_TIMESTAMP(MAX(next_uploaded_at)::INT)
    )                                       AS valid_to
  FROM base
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, checksum_group

)

SELECT
  *,
  md5(cast(coalesce(cast(project_ci_cd_settings_snapshot_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(valid_from as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))                                              AS project_ci_cd_settings_snapshot_pk
FROM grouped