
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_credit_held
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_credit_held_source

)

SELECT *
FROM source
    )
;


  