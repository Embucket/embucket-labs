WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora_stitch.productrateplanchargetier

), renamed AS (

    SELECT 
      productrateplanchargeid AS product_rate_plan_charge_id,
      currency                AS currency,
      price                   AS price,
      active                  AS active
    FROM source
    WHERE deleted = FALSE
    
)

SELECT *
FROM renamed