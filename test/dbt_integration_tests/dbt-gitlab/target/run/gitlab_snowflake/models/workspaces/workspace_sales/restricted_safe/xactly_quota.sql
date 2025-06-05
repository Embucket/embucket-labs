
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_quota
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_quota_source

)

SELECT *
FROM source
    )
;


  