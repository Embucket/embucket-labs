WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_training_completion_dates') }}

)

SELECT
  email::VARCHAR                          AS email_address,
  role::VARCHAR                           AS job_role,
  job_title::VARCHAR                      AS job_title,
  department::VARCHAR                     AS department,
  reporting_to::VARCHAR                   AS reporting_to,
  training::VARCHAR                       AS training_completed,
  knowledge_check_complete::DATE          AS knowledge_check_complete,
  application_exercise_complete::DATE     AS application_exercise_compled_date,
  _updated_at::FLOAT                      AS last_updated_at
FROM source
