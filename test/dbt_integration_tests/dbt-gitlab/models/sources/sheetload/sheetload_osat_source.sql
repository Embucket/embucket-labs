WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','osat') }}
    
), renamed AS (

    SELECT
      TRY_TO_TIMESTAMP_NTZ("TIMESTAMP")::DATE              AS completed_date,
      "EMPLOYEE_NAME"::VARCHAR                             AS employee_name,
      "DIVISION"::VARCHAR                                  AS division,
      NULLIF("SATISFACTION_SCORE",'')::NUMBER              AS satisfaction_score,
      NULLIF("RECOMMEND_TO_FRIEND",'')::NUMBER             AS recommend_to_friend,
      NULLIF(ONBOARDING_BUDDY_EXPERIENCE_SCORE,'')::NUMBER AS buddy_experience_score,
      TRY_TO_TIMESTAMP_NTZ("HIRE_DATE")::DATE              AS hire_date

    FROM source
 
)

SELECT *
FROM renamed
