
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_planrules
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_planrules_source

)

SELECT *
FROM source
    )
;


  