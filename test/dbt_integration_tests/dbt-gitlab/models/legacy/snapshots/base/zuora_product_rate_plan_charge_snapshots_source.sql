WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'zuora_product_rate_plan_charge_snapshots') }}

), renamed AS (

    SELECT 
      id                    AS product_rate_plan_charge_id,
      productrateplanid     AS product_rate_plan_id,
      name                  AS product_rate_plan_charge_name,

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to

    FROM source
    
)

SELECT *
FROM renamed