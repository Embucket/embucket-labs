WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','engineering_speciality_prior_to_capture') }}

), renamed AS (

    SELECT
      "EMPLOYEE_ID"                         AS employee_id,
      "FULL_NAME"                           AS full_name,
      "REPORTS_TO"                          AS reports_to,
      "DIVISION"                            AS division,
      "DEPARTMENT"                          AS department,
      "JOBTITLE_SPECIALITY"                 AS speciality,
      "EFFECTIVE_DATE"::TIMESTAMP::DATE     AS start_date,
      "END_DATE"::TIMESTAMP::DATE           AS end_date
    FROM source

)

SELECT *
FROM renamed
