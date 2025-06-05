
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_comp_order_item
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_comp_order_item_source

)

SELECT *
FROM source
    )
;


  