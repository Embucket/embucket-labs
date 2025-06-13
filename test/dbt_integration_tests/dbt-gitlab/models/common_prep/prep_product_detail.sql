{{ config(
    tags=["mnpi_exception"]
) }}

WITH zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_source') }}

), zuora_product_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_source') }}

), sfdc_zuora_product AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_zproduct_source') }}

), sfdc_zuora_product_rate_plan AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_product_rate_plan_source') }}

), zuora_product_rate_plan_charge_tier AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_tier_source') }}

), common_product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}

), common_product_tier_mapping AS (

    SELECT *
    FROM {{ ref('map_product_tier') }}

), joined AS (

    SELECT
      -- ids
      zuora_product_rate_plan_charge.product_rate_plan_charge_id                        AS dim_product_detail_id,
      zuora_product.product_id                                                          AS product_id,
      common_product_tier.dim_product_tier_id                                           AS dim_product_tier_id,
      zuora_product_rate_plan.product_rate_plan_id                                      AS product_rate_plan_id,
      zuora_product_rate_plan_charge.product_rate_plan_charge_id                        AS product_rate_plan_charge_id,

      -- fields
      zuora_product_rate_plan.product_rate_plan_name                                    AS product_rate_plan_name,
      zuora_product_rate_plan_charge.product_rate_plan_charge_name                      AS product_rate_plan_charge_name,
      zuora_product.product_name                                                        AS product_name,
      zuora_product.sku                                                                 AS product_sku,
      common_product_tier.product_tier_historical                                       AS product_tier_historical,
      common_product_tier.product_tier_historical_short                                 AS product_tier_historical_short,
      common_product_tier.product_tier_name                                             AS product_tier_name,
      common_product_tier.product_tier_name_short                                       AS product_tier_name_short,
      common_product_tier_mapping.product_delivery_type                                 AS product_delivery_type,
      common_product_tier_mapping.product_deployment_type                               AS product_deployment_type,
      common_product_tier_mapping.product_category                                      AS product_category,
      sfdc_zuora_product_rate_plan.product_category                                     AS product_rate_plan_category,
      zuora_product_rate_plan_charge.charge_type                                        AS charge_type,
      CASE
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                                                                               AS service_type,
      LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE '%reporter access%'    AS is_reporter_license,
      zuora_product.effective_start_date                                                AS effective_start_date,
      zuora_product.effective_end_date                                                  AS effective_end_date,
      common_product_tier_mapping.product_ranking                                       AS product_ranking,
      CASE
        WHEN LOWER(zuora_product_rate_plan.product_rate_plan_name) LIKE ANY ('%oss%', '%edu%')
          THEN TRUE
        ELSE FALSE
      END                                                                               AS is_oss_or_edu_rate_plan,
      CASE
        WHEN common_product_tier.product_tier_name = 'Storage' 
            OR product_rate_plan_charge_name LIKE 'Dedicated - Administration%' 
            OR product_rate_plan_charge_name LIKE '%Storage%'
            OR product_rate_plan_charge_name LIKE 'Success Advanced%'
            OR product_rate_plan_charge_name LIKE 'Hosted Runners%'
            OR product_rate_plan_charge_name = 'GitLab Dedicated for US Public Sector - Administration Fee [Small] - 1 Year'
        THEN FALSE
        ELSE TRUE
      END                                                                               AS is_licensed_user,
      CASE
        WHEN common_product_tier_mapping.product_category != 'Base Products' THEN FALSE
        WHEN zuora_product_rate_plan_charge.product_rate_plan_charge_name IN (
          'Max Enrollment',
          'GitLab Dedicated for US Public Sector - Storage 10GB - 1 Year',
          '12x5 US Citizen Support - 3 Year',
          '12x5 US Citizen Support - Monthly',
          '12x5 US Citizen Support - 2 Year',
          '12x5 US Citizen Support - 1 Year',
          '[OSS Program] Self-Managed - Support - 1 Year',
          '24x7 US Citizen Support - 1 Year',
          '24x7 US Citizen Support - Monthly'
          )
        THEN FALSE
        ELSE is_licensed_user
      END                                                                               AS is_licensed_user_base_product,
      CASE
        WHEN is_licensed_user = TRUE AND is_licensed_user_base_product = FALSE THEN TRUE
        ELSE FALSE
      END                                                                               AS is_licensed_user_add_on,
      CASE
        WHEN common_product_tier.product_tier_name = 'Storage' 
            OR LOWER(zuora_product_rate_plan.product_rate_plan_name) ILIKE ANY ('%oss%', '%edu%')
            OR product_rate_plan_charge_name LIKE '%Administration%' 
            OR product_rate_plan_charge_name LIKE '%Storage%'
            OR product_rate_plan_charge_name LIKE 'Success Advanced%'
            OR product_rate_plan_charge_name LIKE 'Hosted Runners%'
        THEN FALSE
        ELSE TRUE
      END                                                                               AS is_arpu,
      MIN(zuora_product_rate_plan_charge_tier.price)                                    AS billing_list_price
    FROM zuora_product
    INNER JOIN sfdc_zuora_product 
      ON sfdc_zuora_product.zqu_sku = zuora_product.sku
    LEFT JOIN sfdc_zuora_product_rate_plan 
      ON sfdc_zuora_product.zqu_zproduct_id = sfdc_zuora_product_rate_plan.zqu_zproduct_id 
    INNER JOIN zuora_product_rate_plan
      ON zuora_product.product_id = zuora_product_rate_plan.product_id
      AND zuora_product_rate_plan.product_rate_plan_id = sfdc_zuora_product_rate_plan.zqu_zuora_id
    INNER JOIN zuora_product_rate_plan_charge
      ON zuora_product_rate_plan.product_rate_plan_id = zuora_product_rate_plan_charge.product_rate_plan_id
    INNER JOIN zuora_product_rate_plan_charge_tier
      ON zuora_product_rate_plan_charge.product_rate_plan_charge_id = zuora_product_rate_plan_charge_tier.product_rate_plan_charge_id
    LEFT JOIN common_product_tier_mapping
      ON zuora_product_rate_plan_charge.product_rate_plan_id = common_product_tier_mapping.product_rate_plan_id
    LEFT JOIN common_product_tier
      ON common_product_tier_mapping.product_tier_historical = common_product_tier.product_tier_historical
    WHERE zuora_product.is_deleted = FALSE
      AND zuora_product_rate_plan_charge_tier.currency = 'USD'
      AND zuora_product_rate_plan_charge_tier.active = TRUE
    {{ dbt_utils.group_by(n=23) }}
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
      END                                                                               AS annual_billing_list_price
    FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@rakhireddy",
    created_date="2020-12-16",
    updated_date="2025-01-13"
) }}
