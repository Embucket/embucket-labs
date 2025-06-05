
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_position
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_position_source

)

SELECT *
FROM source
    )
;


  