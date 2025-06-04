WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'rate_plan_charge_tier') }}

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