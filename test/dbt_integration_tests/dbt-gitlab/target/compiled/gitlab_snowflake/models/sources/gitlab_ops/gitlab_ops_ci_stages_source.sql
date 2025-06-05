

WITH source AS (

  SELECT *
  FROM EMBUCKET.tap_postgres.gitlab_ops_db_ci_stages
  WHERE created_at IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
  
    

), renamed AS (
  
  SELECT
    id::NUMBER            AS ci_stage_id,
    project_id::NUMBER    AS project_id,
    pipeline_id::NUMBER   AS pipeline_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    name::VARCHAR         AS ci_stage_name,
    status::NUMBER        AS ci_stage_status,
    lock_version::NUMBER  AS lock_version,
    position::NUMBER      AS position
  FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at