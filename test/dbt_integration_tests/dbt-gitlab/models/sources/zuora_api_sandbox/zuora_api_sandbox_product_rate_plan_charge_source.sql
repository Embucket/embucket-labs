WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'dup_zuora_api_sandbox_stitch_productrateplancharge') }}

), renamed AS (

    SELECT 
      id                    AS product_rate_plan_charge_id,
      productrateplanid     AS product_rate_plan_id,
      name                  AS product_rate_plan_charge_name
    FROM source
    
)

SELECT *
FROM renamed
