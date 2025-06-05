
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_period
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_period_source

)

SELECT *
FROM source
    )
;


  