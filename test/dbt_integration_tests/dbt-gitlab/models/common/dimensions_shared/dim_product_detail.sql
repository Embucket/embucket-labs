{{ config({
        "alias": "dim_product_detail",
        "tags": "mnpi_exception"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ ref('prep_product_detail') }}

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
      product_category                 AS product_category,
      product_rate_plan_category       AS product_rate_plan_category,
      service_type                     AS service_type,
      charge_type                      AS charge_type,
      is_reporter_license              AS is_reporter_license,
      effective_start_date             AS effective_start_date,
      effective_end_date               AS effective_end_date,
      product_ranking                  AS product_ranking,
      is_oss_or_edu_rate_plan          AS is_oss_or_edu_rate_plan,
      billing_list_price               AS billing_list_price,
      annual_billing_list_price        AS annual_billing_list_price,
      is_licensed_user                 AS is_licensed_user,
      is_licensed_user_base_product    AS is_licensed_user_base_product,
      is_licensed_user_add_on          AS is_licensed_user_add_on,
      is_arpu                          AS is_arpu
    FROM base
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@mdrussell",
    created_date="2020-12-16",
    updated_date="2024-12-03"
) }}
