
  
    

create or replace transient table EMBUCKET.sfdc.sfdc_user_roles_source
    

    
    as (with base as (

    SELECT * 
    FROM EMBUCKET.salesforce_v2_stitch.userrole

)

SELECT *
FROM base
    )
;


  