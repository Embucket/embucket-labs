
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_job_roles_prior_to_2020_02_source
    

    
    as (WITH source AS (

    SELECT * 
    FROM EMBUCKET.sheetload.job_roles_prior_to_2020_02
    
), final AS (

    SELECT 
      job_title,
      job_role
    FROM source
      
) 

SELECT * 
FROM final
    )
;


  