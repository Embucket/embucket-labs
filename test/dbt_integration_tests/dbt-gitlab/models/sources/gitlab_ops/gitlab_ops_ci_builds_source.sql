WITH source AS (

  SELECT *
  FROM {{ source('gitlab_ops', 'ci_builds') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

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
    upstream_pipeline_id::NUMBER      AS upstream_pipeline_id
  FROM source

)


SELECT *
FROM renamed
ORDER BY updated_at

