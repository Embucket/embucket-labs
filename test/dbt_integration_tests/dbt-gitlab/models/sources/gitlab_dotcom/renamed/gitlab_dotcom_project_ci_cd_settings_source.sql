WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_ci_cd_settings_dedupe_source') }}

),

renamed AS (

  SELECT

    id::NUMBER                                             AS project_ci_cd_settings_id,
    project_id::NUMBER                                     AS project_id,
    group_runners_enabled::BOOLEAN                         AS group_runners_enabled,
    merge_pipelines_enabled::BOOLEAN                       AS merge_pipelines_enabled,
    default_git_depth:NUMBER                               AS default_git_depth,
    forward_deployment_enabled::BOOLEAN                    AS forward_deployment_enabled,
    merge_trains_enabled::BOOLEAN                          AS merge_trains_enabled,
    auto_rollback_enabled::BOOLEAN                         AS auto_rollback_enabled,
    keep_latest_artifact::BOOLEAN                          AS keep_latest_artifact,
    restrict_user_defined_variables::BOOLEAN               AS restrict_user_defined_variables,
    job_token_scope_enabled::BOOLEAN                       AS job_token_scope_enabled,
    runner_token_expiration_interval::NUMBER               AS runner_token_expiration_interval,
    separated_caches::BOOLEAN                              AS separated_caches,
    allow_fork_pipelines_to_run_in_parent_project::BOOLEAN AS allow_fork_pipelines_to_run_in_parent_project,
    inbound_job_token_scope_enabled::BOOLEAN               AS inbound_job_token_scope_enabled

  FROM source

)

SELECT *
FROM renamed
