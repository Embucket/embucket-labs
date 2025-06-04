{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('fct_available_to_renew_snapshot_model', 'fct_available_to_renew_snapshot_model'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_date', 'dim_date')

]) }},

-- Day 5 ATR Detail (New SKU) with the last_in_lineage field showing the latest subscription in lineage (including legacy ramps, change of entity, contract resets, late renewal, debook and rebook)

-- PART 1 of the new mart - identifying the standalone subscriptions that have been renewed based on the zuora_renewal_subscription_name

renewal_linking AS (

  SELECT *
  FROM dim_subscription
  WHERE zuora_renewal_subscription_name != '' -- where renewal is not empty, renewal is populated, this is last in lineage FALSE, already renewed

),

is_last_in_lineage AS (

  SELECT DISTINCT
    subscription_name,
    'FALSE' AS is_last_in_lineage
  FROM renewal_linking

),

-- PART 2 - the actual 'Day 5 ATR Detail (New SKU)'

snapshot_dates AS (

  SELECT DISTINCT
    first_day_of_month,
    CASE WHEN first_day_of_month < '2024-03-01'
        THEN snapshot_date_fpa
      ELSE snapshot_date_fpa_fifth
    END AS snapshot_date_fpa
  FROM dim_date

),

fct_available_to_renew_snapshot AS (

  SELECT *
  FROM fct_available_to_renew_snapshot_model

),

prep AS (

  SELECT
    fct_available_to_renew_snapshot.*,
    snapshot_dates.first_day_of_month,
    dim_product_detail.product_tier_name,
    dim_product_detail.product_delivery_type
  FROM fct_available_to_renew_snapshot
  INNER JOIN snapshot_dates
    ON fct_available_to_renew_snapshot.snapshot_date = snapshot_dates.snapshot_date_fpa
  LEFT JOIN dim_product_detail
    ON fct_available_to_renew_snapshot.dim_product_detail_id = dim_product_detail.dim_product_detail_id

),

final AS (

  SELECT
    snapshot_date,
    fiscal_year                                            AS renewal_fiscal_year,
    renewal_month,
    fiscal_quarter_name_fy,
    (CASE WHEN dim_product_detail.product_rate_plan_name ILIKE '%12x5%' THEN '12x5'
      WHEN dim_product_detail.product_rate_plan_name ILIKE '%24x7%' THEN '24x7'
      WHEN dim_product_detail.product_rate_plan_category ILIKE '%duo%' THEN 'Duo'
      WHEN dim_product_detail.product_rate_plan_category ILIKE '%enterprise agile planning%' THEN 'Enterprise Agile Planning'
      WHEN dim_product_detail.product_rate_plan_category ILIKE '%dedicated%' THEN 'Dedicated - Ultimate'
      WHEN prep.product_tier_name ILIKE '%premium%' THEN 'Premium'
      WHEN prep.product_tier_name ILIKE '%ultimate%' THEN 'Ultimate'
      WHEN prep.product_tier_name ILIKE '%bronze%' OR prep.product_tier_name ILIKE '%starter%' THEN 'Bronze/Starter'
      ELSE prep.product_tier_name
    END)                                                   AS product_name,
    prep.product_tier_name,
    prep.product_delivery_type,
    COALESCE (is_last_in_lineage.is_last_in_lineage, TRUE) AS is_last_in_lineage,
    SUM(arr)                                               AS arr
  FROM prep
  LEFT JOIN dim_product_detail
    ON prep.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN is_last_in_lineage
    ON prep.subscription_name = is_last_in_lineage.subscription_name
  {{ dbt_utils.group_by(8) }}
  ORDER BY 1 DESC, 2 DESC, 3, 4, 5

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2025-01-07",
updated_date="2025-01-07"
) }}
