WITH source AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), with_product_category AS (

    SELECT *,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category') }}
    FROM source

)

SELECT *
FROM with_product_category
