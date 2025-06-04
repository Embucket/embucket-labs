WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'rate_plan_charge_tier') }}

), renamed AS (

    SELECT 
      rateplanchargeid        AS rate_plan_charge_id,
      productrateplanchargeid AS product_rate_plan_charge_id,
      price,
      currency
    FROM source
    
)

SELECT *
FROM renamed