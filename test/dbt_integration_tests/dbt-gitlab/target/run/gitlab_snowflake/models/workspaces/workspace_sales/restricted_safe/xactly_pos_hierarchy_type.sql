
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_pos_hierarchy_type
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_pos_hierarchy_type_source

)

SELECT *
FROM source
    )
;


  