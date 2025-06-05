
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_pos_rel_type_hist
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_pos_rel_type_hist_source

)

SELECT *
FROM source
    )
;


  