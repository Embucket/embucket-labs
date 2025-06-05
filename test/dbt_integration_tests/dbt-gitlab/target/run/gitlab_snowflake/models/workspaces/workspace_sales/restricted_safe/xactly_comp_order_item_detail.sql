
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_comp_order_item_detail
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_comp_order_item_detail_source

)

SELECT *
FROM source
    )
;


  