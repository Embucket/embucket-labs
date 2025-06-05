
  
    

create or replace transient table EMBUCKET.zuora_central_sandbox.zuora_central_sandbox_rate_plan_charge_tier_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora_central_sandbox_fivetran.rate_plan_charge_tier

), renamed AS (

    SELECT 
      rate_plan_charge_id         AS rate_plan_charge_id,
      product_rate_plan_charge_id AS product_rate_plan_charge_id,
      price,
      currency
    FROM source
    
)

SELECT *
FROM renamed
    )
;


  