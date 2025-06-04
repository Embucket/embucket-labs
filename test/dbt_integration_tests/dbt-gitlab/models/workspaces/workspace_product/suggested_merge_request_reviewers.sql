
WITH source AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_merge_request_predictions_source')}}

)

SELECT *
FROM source
