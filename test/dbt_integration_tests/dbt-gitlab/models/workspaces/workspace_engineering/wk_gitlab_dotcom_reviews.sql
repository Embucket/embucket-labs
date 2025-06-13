WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_reviews_source') }}

)

SELECT *
FROM source
