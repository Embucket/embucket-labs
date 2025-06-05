
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_pos_hierarchy
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_pos_hierarchy_source

)

SELECT *
FROM source
    )
;


  