
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_user
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_user_source

)

SELECT *
FROM source
    )
;


  