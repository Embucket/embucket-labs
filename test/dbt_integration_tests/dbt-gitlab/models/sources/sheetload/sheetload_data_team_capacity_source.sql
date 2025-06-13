WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','data_team_capacity') }}

), final AS (
    
    SELECT 
        TRY_TO_NUMBER(milestone_id)                 AS milestone_id,
        NULLIF(gitlab_handle, '')::VARCHAR          AS gitlab_handle, 
        TRY_TO_NUMBER(capacity)                     AS capacity
    FROM source

) 

SELECT * 
FROM final
