WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','abuse_top_storage_data') }}

), final AS (
    
    SELECT 
      NULLIF(tracked_date, '')::VARCHAR::DATE                           AS tracked_date,
      TRY_TO_NUMBER(legit_users)                                        AS legit_users,
      TRY_TO_DECIMAL(legit_gb)                                          AS legit_gb,
      TRY_TO_NUMBER(blocked_users)                                      AS blocked_users,
      TRY_TO_DECIMAL(blocked_gb)                                        AS blocked_gb
    FROM source

) 

SELECT * 
FROM final
