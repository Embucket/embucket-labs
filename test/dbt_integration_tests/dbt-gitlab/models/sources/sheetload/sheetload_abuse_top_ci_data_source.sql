WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','abuse_top_ci_data') }}

), final AS (
    
    SELECT 
      NULLIF(tracked_date, '')::VARCHAR::DATE                           AS tracked_date,
      TRY_TO_NUMBER(legit_users)                                        AS legit_users,
      TRY_TO_NUMBER(legit_hours)                                        AS legit_hours,
      TRY_TO_NUMBER(blocked_users)                                      AS blocked_users,
      TRY_TO_NUMBER(blocked_hours)                                      AS blocked_hours
    FROM source

) 

SELECT * 
FROM final
