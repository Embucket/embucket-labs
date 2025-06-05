
  
    

create or replace transient table EMBUCKET.restricted_safe_legacy.zuora_invoice_item
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora.zuora_invoice_item_source

)

SELECT *
FROM source
WHERE is_deleted = FALSE
    )
;


  