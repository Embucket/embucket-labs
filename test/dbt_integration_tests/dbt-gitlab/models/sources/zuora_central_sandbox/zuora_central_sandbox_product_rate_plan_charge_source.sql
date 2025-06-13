WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'product_rate_plan_charge') }}

), renamed AS (

    SELECT 
      id                    AS product_rate_plan_charge_id,
      product_rate_plan_id  AS product_rate_plan_id,
      name                  AS product_rate_plan_charge_name
    FROM source
    
)

SELECT *
FROM renamed