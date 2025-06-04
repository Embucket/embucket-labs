WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product_rate_plan_charge') }}

), renamed AS (

    SELECT 
      id                    AS product_rate_plan_charge_id,
      productrateplanid     AS product_rate_plan_id,
      name                  AS product_rate_plan_charge_name,
      chargetype            AS charge_type
    FROM source
    
)

SELECT *
FROM renamed