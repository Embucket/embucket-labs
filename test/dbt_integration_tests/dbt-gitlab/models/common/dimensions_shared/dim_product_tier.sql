WITH prep_product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}

), final AS (

  SELECT
    dim_product_tier_id,
    product_tier_historical,
    product_tier_historical_short,
    product_tier_name,
    product_tier_name_short,
    product_delivery_type,
    product_deployment_type,
    product_ranking
  FROM prep_product_tier
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@jpeguero",
    created_date="2020-12-28",
    updated_date="2023-05-25"
) }}
