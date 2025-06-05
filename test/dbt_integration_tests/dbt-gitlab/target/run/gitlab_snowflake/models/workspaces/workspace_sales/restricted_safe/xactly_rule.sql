
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_rule
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_rule_source

)

SELECT *
FROM source
    )
;


  