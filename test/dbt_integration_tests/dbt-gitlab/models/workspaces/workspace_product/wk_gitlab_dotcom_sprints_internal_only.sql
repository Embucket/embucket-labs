WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_sprints_internal_only_source') }}

)

SELECT *
FROM source
