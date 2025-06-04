WITH source AS (

  SELECT * 
  FROM {{ source('workday','gitlab_usernames') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT 
    workday_id::VARCHAR                  AS workday_id,
    effective_date::DATE                 AS effective_date,    
    employee_id::NUMBER                  AS employee_id,
    gitlab_username::VARCHAR             AS gitlab_username,
    date_time_completed::TIMESTAMP       AS date_time_completed,
    _fivetran_deleted::BOOLEAN           AS is_deleted,
    _fivetran_synced::TIMESTAMP          AS uploaded_at
  FROM source

)

SELECT * 
FROM renamed
