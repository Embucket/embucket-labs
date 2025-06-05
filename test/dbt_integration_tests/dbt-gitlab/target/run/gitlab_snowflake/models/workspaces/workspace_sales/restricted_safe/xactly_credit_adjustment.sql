
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_credit_adjustment
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_credit_adjustment_source

)

SELECT *
FROM source
    )
;


  