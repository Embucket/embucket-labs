WITH all_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds_dedupe_source') }}

), internal_rows_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds_internal_only_dedupe_source') }}

), all_rows_renamed AS (

  SELECT
    id::NUMBER                        AS ci_build_id,
    status::VARCHAR                   AS status,
    finished_at::TIMESTAMP            AS finished_at,
    created_at::TIMESTAMP             AS created_at,
    updated_at::TIMESTAMP             AS updated_at,
    started_at::TIMESTAMP             AS started_at,
    runner_id::NUMBER                 AS ci_build_runner_id,
    coverage::VARCHAR                 AS coverage,
    commit_id::NUMBER                 AS ci_build_commit_id,
    name::VARCHAR                     AS ci_build_name,
    options::VARCHAR                  AS options,
    allow_failure::VARCHAR            AS allow_failure,
    stage::VARCHAR                    AS stage,
    trigger_request_id::NUMBER        AS ci_build_trigger_request_id,
    stage_idx::NUMBER                 AS stage_idx,
    tag::VARCHAR                      AS tag,
    ref::VARCHAR                      AS ref,
    user_id::NUMBER                   AS ci_build_user_id,
    type::VARCHAR                     AS type,
    description::VARCHAR              AS description,
    project_id::NUMBER                AS ci_build_project_id,
    erased_by_id::NUMBER              AS ci_build_erased_by_id,
    erased_at::TIMESTAMP              AS ci_build_erased_at,
    artifacts_expire_at::TIMESTAMP    AS ci_build_artifacts_expire_at,
    environment::VARCHAR              AS environment,
    queued_at::TIMESTAMP              AS ci_build_queued_at,
    lock_version::VARCHAR             AS lock_version,
    coverage_regex::VARCHAR           AS coverage_regex,
    auto_canceled_by_id::NUMBER       AS ci_build_auto_canceled_by_id,
    retried::BOOLEAN                  AS retried,
    stage_id::NUMBER                  AS ci_build_stage_id,
    protected::BOOLEAN                AS protected,
    failure_reason::VARCHAR           AS failure_reason,
    scheduled_at::TIMESTAMP           AS ci_build_scheduled_at,
    upstream_pipeline_id::NUMBER      AS upstream_pipeline_id,
    scheduling_type::NUMBER           AS scheduling_type

  FROM all_rows_source

), internal_rows_renamed AS (

  SELECT
    id::NUMBER                        AS internal_ci_build_id,
    updated_at::TIMESTAMP             AS internal_updated_at,
    name::VARCHAR                     AS internal_ci_build_name,
    stage::VARCHAR                    AS internal_stage,
    ref::VARCHAR                      AS internal_ref,
    description::VARCHAR              AS internal_description,
    project_id::NUMBER                AS internal_project_id
  from internal_rows_source

), joined AS (

  SELECT

    ci_build_id                                       AS ci_build_id,
    status                                            AS status,
    finished_at                                       AS finished_at,
    created_at                                        AS created_at,
    updated_at                                        AS updated_at,
    started_at                                        AS started_at,
    ci_build_runner_id                                AS ci_build_runner_id,
    coverage                                          AS coverage,
    ci_build_commit_id                                AS ci_build_commit_id,
    COALESCE(ci_build_name, internal_ci_build_name)   AS ci_build_name,
    options                                           AS options,
    allow_failure                                     AS allow_failure,
    internal_stage                                    AS stage,
    ci_build_trigger_request_id                       AS ci_build_trigger_request_id,
    stage_idx                                         AS stage_idx,
    tag                                               AS tag,
    internal_ref                                      AS ref,
    ci_build_user_id                                  AS ci_build_user_id,
    type                                              AS type,
    internal_description                              AS description,
    ci_build_project_id                               AS ci_build_project_id,
    ci_build_erased_by_id                             AS ci_build_erased_by_id,
    ci_build_erased_at                                AS ci_build_erased_at,
    ci_build_artifacts_expire_at                      AS ci_build_artifacts_expire_at,
    environment                                       AS environment,
    ci_build_queued_at                                AS ci_build_queued_at,
    lock_version                                      AS lock_version,
    coverage_regex                                    AS coverage_regex,
    ci_build_auto_canceled_by_id                      AS ci_build_auto_canceled_by_id,
    retried                                           AS retried,
    ci_build_stage_id                                 AS ci_build_stage_id,
    protected                                         AS protected,
    failure_reason                                    AS failure_reason,
    ci_build_scheduled_at                             AS ci_build_scheduled_at,
    upstream_pipeline_id                              AS upstream_pipeline_id,
    scheduling_type                                   AS scheduling_type

  FROM all_rows_renamed
  LEFT JOIN internal_rows_renamed 
    ON all_rows_renamed.ci_build_id = internal_rows_renamed.internal_ci_build_id

)

SELECT *
FROM joined
