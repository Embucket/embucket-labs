
  
    

create or replace transient table EMBUCKET.legacy.sfdc_user_roles
    

    
    as (with base as (

    SELECT * 
    FROM EMBUCKET.sfdc.sfdc_user_roles_source

)

SELECT *
FROM base
    )
;


  