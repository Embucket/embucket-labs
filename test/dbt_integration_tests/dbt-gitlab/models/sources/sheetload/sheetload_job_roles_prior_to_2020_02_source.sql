WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','job_roles_prior_to_2020_02') }}
    
), final AS (

    SELECT 
      job_title,
      job_role
    FROM source
      
) 

SELECT * 
FROM final
