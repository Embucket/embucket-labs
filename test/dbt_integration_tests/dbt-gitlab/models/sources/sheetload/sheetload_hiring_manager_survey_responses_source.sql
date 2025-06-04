WITH source AS (

  SELECT *
  FROM {{ source('sheetload','hiring_manager_survey_responses') }}

),

renamed AS (

  SELECT
    timestamp::TIMESTAMP AS responded_at,
    overall_rating::NUMBER AS overall_rating,
    additional_feedback::VARCHAR AS additional_feedback,
    "sourced_by_TA"::VARCHAR AS sourced_by_ta,
    recruiter_communication::NUMBER AS recruiter_communication,
    "TA_customer_service_rating"::NUMBER AS ta_customer_service_rating
  FROM source

)

SELECT *
FROM renamed
