WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_sources_pipelines_dedupe_source') }}

), renamed AS (

  SELECT
    id::NUMBER                   AS ci_source_pipeline_id,
    project_id::NUMBER           AS project_id,
    pipeline_id::NUMBER          AS pipeline_id,
    source_project_id::NUMBER    AS source_project_id,
    source_pipeline_id::NUMBER   AS source_pipeline_id,
    source_job_id::NUMBER        AS source_job_id
  FROM source

)

SELECT *
FROM renamed
