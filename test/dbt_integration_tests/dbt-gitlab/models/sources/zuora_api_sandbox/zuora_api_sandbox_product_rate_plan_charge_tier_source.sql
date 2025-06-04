WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'product_rate_plan_charge_tier') }}

), renamed AS (

    SELECT 
      productrateplanchargeid AS product_rate_plan_charge_id,
      currency                AS currency,
      price                   AS price
    FROM source
    
)

SELECT *
FROM renamed