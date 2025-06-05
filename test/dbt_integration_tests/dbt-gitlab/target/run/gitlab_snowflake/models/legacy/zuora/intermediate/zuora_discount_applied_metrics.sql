
  
    

create or replace transient table EMBUCKET.legacy.zuora_discount_applied_metrics
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora.zuora_discount_applied_metrics_source

)

SELECT *
FROM source
    )
;


  