
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_plan_assignment
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_plan_assignment_source

)

SELECT *
FROM source
    )
;


  