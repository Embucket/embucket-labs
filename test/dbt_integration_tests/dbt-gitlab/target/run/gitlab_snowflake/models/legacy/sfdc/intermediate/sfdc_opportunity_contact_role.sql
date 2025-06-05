
  
    

create or replace transient table EMBUCKET.legacy.sfdc_opportunity_contact_role
    

    
    as (WITH base AS (

    SELECT *
    FROM EMBUCKET.sfdc.sfdc_opportunity_contact_role_source

)

SELECT *
FROM base
    )
;


  