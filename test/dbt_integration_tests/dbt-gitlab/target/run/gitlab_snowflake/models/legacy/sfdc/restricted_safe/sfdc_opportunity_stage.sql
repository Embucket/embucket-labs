
  
    

create or replace transient table EMBUCKET.restricted_safe_legacy.sfdc_opportunity_stage
    

    
    as (WITH base AS (

    SELECT *
    FROM EMBUCKET.sfdc.sfdc_opportunity_stage_source

)

SELECT *
FROM base
    )
;


  