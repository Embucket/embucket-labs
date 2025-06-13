WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ml_experiments_source') }}

)

SELECT *
FROM source
