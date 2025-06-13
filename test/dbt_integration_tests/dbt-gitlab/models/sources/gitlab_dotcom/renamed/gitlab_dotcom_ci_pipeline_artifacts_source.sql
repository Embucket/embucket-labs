WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_artifacts_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER             AS ci_pipeline_artifact_id,
    project_id::NUMBER     AS project_id,
    pipeline_id::NUMBER    AS pipeline_id,
    file_type::NUMBER      AS file_type,
    size::NUMBER           AS size,
    created_at::TIMESTAMP  AS created_at,
    updated_at::TIMESTAMP  AS updated_at,
    expire_at::TIMESTAMP   AS expire_at,
    file::VARCHAR          AS file,
    file_store::NUMBER     AS file_store,
    file_format::NUMBER    AS file_format,
    locked::NUMBER         AS locked
  FROM source

)


SELECT *
FROM renamed
