{{ config(
    tags=["mnpi"]
) }}

WITH zuora_central_sandbox_product AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_source') }}

), zuora_central_sandbox_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_rate_plan_source') }}

), zuora_central_sandbox_product_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_rate_plan_charge_source') }}

), zuora_central_sandbox_product_rate_plan_charge_tier AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_rate_plan_charge_tier_source') }}

), common_product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier_central_sandbox') }}

), common_product_tier_mapping AS (

    SELECT *
    FROM {{ ref('map_product_tier_central_sandbox') }}

), joined AS (

    SELECT
      -- ids
      zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_charge_id                        AS dim_product_detail_id,
      zuora_central_sandbox_product.product_id                                                          AS product_id,
      common_product_tier.dim_product_tier_id                                                           AS dim_product_tier_id,
      zuora_central_sandbox_product_rate_plan.product_rate_plan_id                                      AS product_rate_plan_id,
      zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_charge_id                        AS product_rate_plan_charge_id,

      -- fields
      zuora_central_sandbox_product_rate_plan.product_rate_plan_name                                    AS product_rate_plan_name,
      zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_charge_name                      AS product_rate_plan_charge_name,
      zuora_central_sandbox_product.product_name                                                        AS product_name,
      zuora_central_sandbox_product.sku                                                                 AS product_sku,
      common_product_tier.product_tier_historical                                                       AS product_tier_historical,
      common_product_tier.product_tier_historical_short                                                 AS product_tier_historical_short,
      common_product_tier.product_tier_name                                                             AS product_tier_name,
      common_product_tier.product_tier_name_short                                                       AS product_tier_name_short,
      common_product_tier_mapping.product_delivery_type                                                 AS product_delivery_type,
      common_product_tier_mapping.product_deployment_type                                               AS product_deployment_type,
      common_product_tier_mapping.product_category                                                      AS product_category,
      CASE
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                                                                                               AS service_type,
      LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%reporter access%'    AS is_reporter_license,
      zuora_central_sandbox_product.effective_start_date                                                AS effective_start_date,
      zuora_central_sandbox_product.effective_end_date                                                  AS effective_end_date,
      common_product_tier_mapping.product_ranking                                                       AS product_ranking,
      CASE
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE ANY ('%oss%', '%edu%')
          THEN TRUE
        ELSE FALSE
      END                                                                                               AS is_oss_or_edu_rate_plan,
      MIN(zuora_central_sandbox_product_rate_plan_charge_tier.price)                                    AS billing_list_price
    FROM zuora_central_sandbox_product
    INNER JOIN zuora_central_sandbox_product_rate_plan
      ON zuora_central_sandbox_product.product_id = zuora_central_sandbox_product_rate_plan.product_id
    INNER JOIN zuora_central_sandbox_product_rate_plan_charge
      ON zuora_central_sandbox_product_rate_plan.product_rate_plan_id = zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_id
    INNER JOIN zuora_central_sandbox_product_rate_plan_charge_tier
      ON zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_charge_id = zuora_central_sandbox_product_rate_plan_charge_tier.product_rate_plan_charge_id
    LEFT JOIN common_product_tier_mapping
      ON zuora_central_sandbox_product_rate_plan_charge.product_rate_plan_id = common_product_tier_mapping.product_rate_plan_id
    LEFT JOIN common_product_tier
      ON common_product_tier_mapping.product_tier_historical = common_product_tier.product_tier_historical
    WHERE zuora_central_sandbox_product.is_deleted = FALSE
      AND zuora_central_sandbox_product_rate_plan_charge_tier.currency = 'USD'
    {{ dbt_utils.group_by(n=21) }}
    ORDER BY 1, 3

), final AS (--add annualized billing list price

    SELECT
      joined.*,
      CASE
        WHEN LOWER(product_rate_plan_name)          LIKE '%month%'
          OR LOWER(product_rate_plan_charge_name)   LIKE '%month%'
          OR LOWER(product_name)                    LIKE '%month%'
          THEN (billing_list_price*12)
        WHEN LOWER(product_rate_plan_name)          LIKE '%2 year%'
          OR LOWER(product_rate_plan_charge_name)   LIKE '%2 year%'
          OR LOWER(product_name)                    LIKE '%2 year%'
          THEN (billing_list_price/2)
        WHEN LOWER(product_rate_plan_name)          LIKE '%3 year%'
          OR LOWER(product_rate_plan_charge_name)   LIKE '%3 year%'
          OR LOWER(product_name)                    LIKE '%3 year%'
          THEN (billing_list_price/3)
        WHEN LOWER(product_rate_plan_name)          LIKE '%4 year%'
          OR LOWER(product_rate_plan_charge_name)   LIKE '%4 year%'
          OR LOWER(product_name)                    LIKE '%4 year%'
          THEN (billing_list_price/4)
        WHEN LOWER(product_rate_plan_name)          LIKE '%5 year%'
          OR LOWER(product_rate_plan_charge_name)   LIKE '%5 year%'
          OR LOWER(product_name)                    LIKE '%5 year%'
          THEN (billing_list_price/5)
        ELSE billing_list_price
      END                                                                                           AS annual_billing_list_price
    FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@snalamaru",
    created_date="2022-03-31",
    updated_date="2024-04-18"
) }}