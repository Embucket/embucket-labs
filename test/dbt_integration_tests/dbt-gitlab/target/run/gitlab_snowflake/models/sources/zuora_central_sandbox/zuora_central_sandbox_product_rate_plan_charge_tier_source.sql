
  
    

create or replace transient table EMBUCKET.zuora_central_sandbox.zuora_central_sandbox_product_rate_plan_charge_tier_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora_central_sandbox_fivetran.product_rate_plan_charge_tier

), renamed AS (

    SELECT 
      product_rate_plan_charge_id           AS product_rate_plan_charge_id,
      currency                              AS currency,
      price                                 AS price
    FROM source
    
)

SELECT *
FROM renamed
    )
;


  