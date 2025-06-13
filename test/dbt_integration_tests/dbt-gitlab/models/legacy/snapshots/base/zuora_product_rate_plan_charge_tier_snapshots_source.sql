WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'zuora_product_rate_plan_charge_tier_snapshots') }}

), renamed AS (

    SELECT 
      productrateplanchargeid AS product_rate_plan_charge_id,
      currency                AS currency,
      price                   AS price,


      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to

    FROM source
    
)

SELECT *
FROM renamed