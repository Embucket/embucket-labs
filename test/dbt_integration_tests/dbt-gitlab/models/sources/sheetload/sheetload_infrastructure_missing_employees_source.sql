WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','infrastructure_missing_employees') }}

), final AS (
    
    SELECT 
      NULLIF(employee_id, '')::INTEGER                                    AS employee_id,
      NULLIF(gitlab_dotcom_user_id, '')::VARCHAR                          AS gitlab_dotcom_user_id,
      NULLIF(full_name, '')::VARCHAR                                      AS full_name,
      NULLIF(work_email, '')::VARCHAR                                     AS work_email
    FROM source

) 

SELECT * 
FROM final
