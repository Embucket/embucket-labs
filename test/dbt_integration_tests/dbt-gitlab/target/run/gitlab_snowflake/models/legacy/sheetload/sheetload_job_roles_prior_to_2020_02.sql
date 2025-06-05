
  
    

create or replace transient table EMBUCKET.legacy.sheetload_job_roles_prior_to_2020_02
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_job_roles_prior_to_2020_02_source

)

SELECT *
FROM source
    )
;


  