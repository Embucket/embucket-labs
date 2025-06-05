
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_role
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_role_source

)

SELECT *
FROM source
    )
;


  