WITH all_rows_source AS (

  SELECT
    id::NUMBER                           AS ci_pipeline_id,
    created_at::TIMESTAMP                AS created_at,
    updated_at::TIMESTAMP                AS updated_at,
    ref::VARCHAR                         AS ref,
    true::BOOLEAN                         AS has_tag,
    yaml_errors::VARCHAR                 AS yaml_errors,
    committed_at::TIMESTAMP              AS committed_at,
    project_id::NUMBER                   AS project_id,
    status::VARCHAR                      AS status,
    started_at::TIMESTAMP                AS started_at,
    finished_at::TIMESTAMP               AS finished_at,
    duration::NUMBER                     AS ci_pipeline_duration,
    user_id::NUMBER                      AS user_id,
    lock_version::NUMBER                 AS lock_version,
    auto_canceled_by_id::NUMBER          AS auto_canceled_by_id,
    pipeline_schedule_id::NUMBER         AS pipeline_schedule_id,
    source::NUMBER                       AS ci_pipeline_source,
    config_source::NUMBER                AS config_source,
    true::BOOLEAN                   AS is_protected,
    failure_reason::VARCHAR              AS failure_reason,
    iid::NUMBER                          AS ci_pipeline_iid,
    merge_request_id::NUMBER             AS merge_request_id,
    true::BOOLEAN              AS is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP AS is_deleted_updated_at
  FROM {{ ref('gitlab_dotcom_ci_pipelines_dedupe_source') }}

), internal_rows_source AS (

  SELECT
    id::NUMBER                      AS internal_ci_pipeline_id,
    ref::VARCHAR                    AS internal_ref
  FROM {{ ref('gitlab_dotcom_ci_pipelines_internal_only_dedupe_source') }} 

), joined AS (

  SELECT
    ci_pipeline_id                  AS ci_pipeline_id,
    created_at                      AS created_at,
    updated_at                      AS updated_at,
    internal_ref                    AS ref,
    has_tag                         AS has_tag,
    yaml_errors                     AS yaml_errors,
    committed_at                    AS committed_at,
    project_id                      AS project_id,
    status                          AS status,
    started_at                      AS started_at,
    finished_at                     AS finished_at,
    ci_pipeline_duration            AS ci_pipeline_duration,
    user_id                         AS user_id,
    lock_version                    AS lock_version,
    auto_canceled_by_id             AS auto_canceled_by_id,
    pipeline_schedule_id            AS pipeline_schedule_id,
    ci_pipeline_source              AS ci_pipeline_source,
    config_source                   AS config_source,
    is_protected                    AS is_protected,
    failure_reason                  AS failure_reason,
    ci_pipeline_iid                 AS ci_pipeline_iid,
    merge_request_id                AS merge_request_id,
    is_deleted                      AS is_deleted,
    is_deleted_updated_at           AS is_deleted_updated_at
  FROM all_rows_source
  LEFT JOIN internal_rows_source
    ON all_rows_source.ci_pipeline_id = internal_rows_source.internal_ci_pipeline_id

)

SELECT *
FROM joined
WHERE created_at IS NOT NULL
