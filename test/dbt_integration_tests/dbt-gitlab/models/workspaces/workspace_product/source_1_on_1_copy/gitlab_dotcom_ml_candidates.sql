WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ml_candidates_source') }}

)

SELECT *
FROM source
