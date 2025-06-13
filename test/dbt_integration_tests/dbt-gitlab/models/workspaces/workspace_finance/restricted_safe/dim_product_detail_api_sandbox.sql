WITH base AS (

    SELECT *
    FROM {{ ref('prep_product_detail_api_sandbox') }}

), final AS (

    SELECT
      dim_product_detail_id            AS dim_product_detail_id,
      product_id                       AS product_id,
      dim_product_tier_id              AS dim_product_tier_id,
      product_rate_plan_id             AS product_rate_plan_id,
      product_rate_plan_charge_id      AS product_rate_plan_charge_id,
      product_rate_plan_name           AS product_rate_plan_name,
      product_rate_plan_charge_name    AS product_rate_plan_charge_name,
      product_name                     AS product_name,
      product_sku                      AS product_sku,
      product_tier_historical          AS product_tier_historical,
      product_tier_historical_short    AS product_tier_historical_short,
      product_tier_name                AS product_tier_name,
      product_tier_name_short          AS product_tier_name_short,
      product_delivery_type            AS product_delivery_type,
      product_deployment_type          AS product_deployment_type,
      service_type                     AS service_type,
      is_reporter_license              AS is_reporter_license,
      effective_start_date             AS effective_start_date,
      effective_end_date               AS effective_end_date,
      product_ranking                  AS product_ranking,
      is_oss_or_edu_rate_plan          AS is_oss_or_edu_rate_plan,
      billing_list_price               AS billing_list_price,
      annual_billing_list_price        AS annual_billing_list_price
    FROM base
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@jpeguero",
    created_date="2021-08-26",
    updated_date="2023-05-26"
) }}
