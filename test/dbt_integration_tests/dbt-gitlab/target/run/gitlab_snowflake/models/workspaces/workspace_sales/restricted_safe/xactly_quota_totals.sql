
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_quota_totals
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_quota_totals_source

)

SELECT *
FROM source
    )
;


  