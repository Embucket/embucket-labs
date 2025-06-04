WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_hiring_manager_survey_responses_source') }}

)

SELECT *
FROM source
