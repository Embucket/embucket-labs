WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_stages_dedupe_source') }}
  WHERE created_at IS NOT NULL

), renamed AS (
  
  SELECT
    id::NUMBER           AS ci_stage_id,
    project_id::NUMBER   AS project_id,
    pipeline_id::NUMBER  AS pipeline_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    name::VARCHAR         AS ci_stage_name,
    status::NUMBER       AS ci_stage_status,
    lock_version::NUMBER AS lock_version,
    position::NUMBER     AS position
  FROM source

)

SELECT *
FROM renamed
