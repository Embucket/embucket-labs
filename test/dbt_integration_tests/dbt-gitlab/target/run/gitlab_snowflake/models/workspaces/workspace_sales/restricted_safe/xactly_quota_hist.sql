
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_quota_hist
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_quota_hist_source

)

SELECT *
FROM source
    )
;


  