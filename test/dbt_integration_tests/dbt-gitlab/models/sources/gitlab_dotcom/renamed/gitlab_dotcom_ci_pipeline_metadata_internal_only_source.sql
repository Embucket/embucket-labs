WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_metadata_internal_only_dedupe_source') }}

),

renamed AS (

  SELECT
    project_id::INT             AS project_id,
    pipeline_id::INT            AS ci_pipeline_id,
    name::VARCHAR               AS name
  FROM source
)

SELECT *
FROM renamed
