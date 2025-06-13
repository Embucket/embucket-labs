WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds_metadata_source') }}

)

SELECT *
FROM source
