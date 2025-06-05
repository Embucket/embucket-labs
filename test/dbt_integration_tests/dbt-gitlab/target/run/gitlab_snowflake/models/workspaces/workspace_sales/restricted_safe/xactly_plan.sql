
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_plan
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_plan_source

)

SELECT *
FROM source
    )
;


  