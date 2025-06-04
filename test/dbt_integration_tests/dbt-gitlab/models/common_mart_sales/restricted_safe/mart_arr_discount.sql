{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('sheetload_partner_discount_summary_source', 'sheetload_partner_discount_summary_source'),
    ('fct_invoice_item', 'fct_invoice_item'),
    ('dim_charge', 'dim_charge'),
    ('dim_product_detail', 'dim_product_detail'),
    ('fct_charge', 'fct_charge'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_crm_user', 'dim_crm_user'),
    ('dim_date', 'dim_date'),
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_deal_path', 'dim_deal_path'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy')
]) }},

sheetload_partner_discount AS (

  SELECT
    dim_crm_opportunity_id,
    MAX(discount_percent) AS partner_margin
  FROM sheetload_partner_discount_summary_source
  GROUP BY 1

),

discount_prep_step_1 AS (

  SELECT
    dim_charge.rate_plan_charge_description,
    fct_charge.list_price,
    fct_charge.extended_list_price,
    CASE
      WHEN '2020-02-29' BETWEEN dim_charge.effective_start_date AND dim_charge.effective_end_date
        THEN DATEDIFF(D, dim_charge.effective_start_date, dim_charge.effective_end_date) - 1
      WHEN '2024-02-29' BETWEEN dim_charge.effective_start_date AND dim_charge.effective_end_date
        THEN DATEDIFF(D, dim_charge.effective_start_date, dim_charge.effective_end_date) - 1
      WHEN '2028-02-29' BETWEEN dim_charge.effective_start_date AND dim_charge.effective_end_date
        THEN DATEDIFF(D, dim_charge.effective_start_date, dim_charge.effective_end_date) - 1
      WHEN '2032-02-29' BETWEEN dim_charge.effective_start_date AND dim_charge.effective_end_date
        THEN DATEDIFF(D, dim_charge.effective_start_date, dim_charge.effective_end_date) - 1
      ELSE DATEDIFF(D, dim_charge.effective_start_date, dim_charge.effective_end_date)
    END                                                                                                        AS effective_days,
    CASE
      WHEN fct_charge.list_price IS NOT NULL
        THEN fct_charge.list_price / NULLIFZERO(fct_invoice_item.quantity)
      WHEN dim_charge.billing_period = 'Month' AND fct_charge.list_price IS NULL
        THEN (fct_charge.extended_list_price / NULLIFZERO(fct_invoice_item.quantity)) / dim_subscription.current_term
      ELSE (fct_charge.extended_list_price * 365 / NULLIFZERO(effective_days)) / NULLIFZERO(fct_invoice_item.quantity)
    END                                                                                                        AS list_price_per_unit,
    fct_invoice_item.arr / NULLIFZERO(fct_invoice_item.quantity)                                               AS arpu,
    fct_invoice_item.invoice_number,
    fct_invoice_item.invoice_item_id,
    fct_invoice_item.is_last_segment_version,
    fct_invoice_item.charge_id,
    fct_invoice_item.invoice_item_charge_amount,
    (fct_invoice_item.quantity - ZEROIFNULL(
      LAG(fct_invoice_item.quantity)
        OVER (
          PARTITION BY fct_invoice_item.invoice_number, dim_charge.rate_plan_charge_number
          ORDER BY fct_invoice_item.quantity ASC, fct_invoice_item.is_last_segment_version DESC, fct_invoice_item.invoice_item_charge_amount DESC
        )
    ))                                                                                                         AS delta_quantity, --delta quantity on add-on orders
    fct_invoice_item.invoice_item_unit_price,
    fct_invoice_item.invoice_date,
    fct_invoice_item.dim_subscription_id,
    fct_invoice_item.dim_product_detail_id,
    fct_invoice_item.mrr,
    fct_invoice_item.arr,
    fct_invoice_item.quantity,
    dim_crm_account.parent_crm_account_name,
    dim_crm_account.is_jihu_account,
    dim_crm_account.is_reseller,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.parent_crm_account_geo,
    dim_crm_account.parent_crm_account_region,
    dim_crm_account.parent_crm_account_upa_country,
    fct_charge.delta_mrc                                                                                       AS delta_mrr,
    dim_charge.specific_billing_period,
    dim_charge.billing_period,
    dim_charge.effective_start_date,
    dim_charge.effective_end_date,
    dim_subscription.subscription_name,
    dim_subscription.term_start_date,
    dim_subscription.term_end_date,
    dim_product_detail.product_name,
    dim_product_detail.product_tier_name,
    dim_product_detail.product_delivery_type,
    dim_product_detail.product_rate_plan_charge_name,
    dim_product_detail.is_oss_or_edu_rate_plan,
    dim_crm_opportunity.dim_crm_opportunity_id,
    dim_crm_opportunity.subscription_type,
    dim_crm_opportunity.resale_partner_name,
    dim_crm_opportunity.opportunity_category,
    fct_crm_opportunity.partner_margin_percentage,
    CASE
      WHEN dim_order_type.order_type_name = '1. New - First Order'
        THEN 'First Order'
      ELSE 'Growth'
    END                                                                                                        AS order_type,
    COALESCE(dim_deal_path.deal_path_name, 'Other')                                                            AS deal_path_name,
    COALESCE(sheetload_partner_discount.partner_margin, (fct_crm_opportunity.partner_margin_percentage / 100)) AS partner_margin
  FROM fct_invoice_item
  LEFT JOIN dim_charge
    ON fct_invoice_item.charge_id = dim_charge.dim_charge_id
  LEFT JOIN dim_product_detail
    ON fct_invoice_item.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN fct_charge
    ON fct_invoice_item.charge_id = fct_charge.dim_charge_id
  INNER JOIN dim_crm_account
    ON fct_invoice_item.dim_crm_account_id_subscription = dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_subscription
    ON fct_invoice_item.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN dim_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN fct_crm_opportunity
    ON dim_crm_opportunity.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_order_type
    ON fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
  LEFT JOIN dim_deal_path
    ON fct_crm_opportunity.dim_deal_path_id = dim_deal_path.dim_deal_path_id
  LEFT JOIN sheetload_partner_discount
    ON dim_crm_opportunity.dim_crm_opportunity_id = sheetload_partner_discount.dim_crm_opportunity_id
  WHERE fct_invoice_item.invoice_date > '2022-01-31'
  ORDER BY fct_invoice_item.quantity ASC, fct_invoice_item.is_last_segment_version DESC, fct_invoice_item.invoice_item_charge_amount DESC
),

discount_prep_step_2 AS (

  --Checking eligibility of use cases to be included in the discount mart

  SELECT
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_month,
    discount_prep_step_1.invoice_date,
    discount_prep_step_1.rate_plan_charge_description,
    discount_prep_step_1.invoice_number,
    discount_prep_step_1.invoice_item_id,
    discount_prep_step_1.product_name,
    discount_prep_step_1.product_tier_name,
    discount_prep_step_1.is_last_segment_version,
    discount_prep_step_1.delta_quantity,
    discount_prep_step_1.quantity,
    discount_prep_step_1.delta_mrr,
    discount_prep_step_1.arpu,
    discount_prep_step_1.invoice_item_charge_amount,
    discount_prep_step_1.billing_period,
    discount_prep_step_1.specific_billing_period,
    discount_prep_step_1.dim_subscription_id,
    discount_prep_step_1.subscription_name,
    discount_prep_step_1.term_start_date,
    discount_prep_step_1.term_end_date,
    discount_prep_step_1.product_delivery_type,
    discount_prep_step_1.order_type,
    discount_prep_step_1.deal_path_name,
    discount_prep_step_1.dim_parent_crm_account_id,
    discount_prep_step_1.parent_crm_account_sales_segment        AS parent_crm_account_segment,
    discount_prep_step_1.parent_crm_account_name,
    discount_prep_step_1.parent_crm_account_geo,
    discount_prep_step_1.parent_crm_account_region,
    discount_prep_step_1.parent_crm_account_upa_country          AS parent_crm_account_country,
    discount_prep_step_1.dim_crm_opportunity_id,
    discount_prep_step_1.subscription_type,
    discount_prep_step_1.opportunity_category,
    discount_prep_step_1.is_jihu_account,
    discount_prep_step_1.is_reseller,
    discount_prep_step_1.resale_partner_name,
    CASE
      WHEN subscription_type = 'Add-On Business'
        THEN delta_quantity
      ELSE quantity
    END                                                          AS quantity_with_addon_update,
    ROUND((CASE
      WHEN billing_period = 'Month'
        THEN list_price_per_unit * 12
      WHEN billing_period = 'Two Years'
        THEN list_price_per_unit / 2
      WHEN billing_period = 'Three Years'
        THEN list_price_per_unit / 3
      WHEN billing_period = 'Four Years'
        THEN list_price_per_unit / 4
      WHEN billing_period = 'Five Years'
        THEN list_price_per_unit / 5
      WHEN billing_period = 'Specific Months'
        THEN list_price_per_unit * 12 / specific_billing_period
      ELSE list_price_per_unit
    END), 2)                                                     AS list_price_per_unit_calc,

    /*Premium price increase was effective from 2023-04-03, however, our systems were not ready for 18% discount coupon by that time,
    and we incoroprated 18% discount on list price itself until 2023-05-18.*/

    CASE
      WHEN LOWER(product_name) LIKE '%premium%' AND subscription_type = 'Add-On Business' AND invoice_date BETWEEN term_start_date AND term_end_date
        AND term_start_date < '2023-04-03' AND arpu <= 228
        THEN 228
      WHEN LOWER(product_name) LIKE '%premium%' AND subscription_type = 'Add-On business' AND invoice_date BETWEEN term_start_date AND term_end_date
        AND (term_start_date >= '2023-04-03' OR arpu > 228)
        THEN 348
      WHEN ROUND(list_price_per_unit_calc) = 285 THEN 348
      ELSE list_price_per_unit_calc
    END                                                          AS list_price_per_unit_overwrite,
    (list_price_per_unit_overwrite * quantity_with_addon_update) AS list_price_calc,
    (arpu * quantity_with_addon_update)                          AS arr_calc,
    (list_price_calc - arr_calc) / NULLIFZERO(list_price_calc)   AS discount_on_deal,
    CASE
      WHEN delta_mrr = 0 AND subscription_type = 'Add-On Business'
        THEN 'NE: Add-on_duplicates'
      WHEN is_oss_or_edu_rate_plan = TRUE
        THEN 'NE: OSS_EDU_Subscriptions'
      WHEN invoice_item_charge_amount < 0
        THEN 'NE: Credit/Decommissions'
      WHEN billing_period = 'Month' AND is_last_segment_version = FALSE
        THEN 'NE: Duplicate_arr_on_monthly_billings'
      WHEN list_price_per_unit IS NULL
        THEN 'NE:List_Price_null'
      WHEN ZEROIFNULL(arr) = 0
        THEN 'NE: ARR_zero'
      WHEN quantity_with_addon_update < 0
        THEN 'NE: Contractions'
      WHEN quantity_with_addon_update = 0
        THEN 'NE: Quantity=0'
      WHEN subscription_name LIKE '%UPLIFT RENEWAL AND TRUEUP'
        THEN 'Deal in FY24-Q2 99.5% discount'
      WHEN discount_on_deal < 0
        THEN 'Negative_discount_data_issue'
      ELSE 'Eligible'
    END                                                          AS discount_eligible_flag,

    --In case where the overall discount is lower than the partner margin, we consider lower of these 2 as partner margin.

    CASE
      WHEN discount_on_deal >= 0 AND discount_on_deal < discount_prep_step_1.partner_margin
        THEN discount_on_deal
      ELSE discount_prep_step_1.partner_margin
    END                                                          AS partner_margin
  FROM discount_prep_step_1
  LEFT JOIN dim_date
    ON discount_prep_step_1.invoice_date = dim_date.date_actual
  WHERE
    discount_eligible_flag = 'Eligible'
    AND fiscal_quarter_name_fy >= 'FY23-Q1'

),

discount_prep_step_3 AS (

  --Calculation for the reseller/partner orders

  SELECT
    *,
    ROUND(arr_calc / (1 - ZEROIFNULL(partner_margin)), 2)                     AS arr_incl_channel_margin,
    (list_price_calc - arr_incl_channel_margin) / NULLIFZERO(list_price_calc) AS end_user_discount
  FROM discount_prep_step_2


),

discount_prep_step_4 AS (

  --Sales Analytics - Discount Analysis Dashboard

  SELECT
    fiscal_quarter_name_fy,
    first_day_of_month                                                    AS invoice_month,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    dim_crm_opportunity_id,
    invoice_number,
    invoice_item_id,
    dim_subscription_id,
    subscription_name,
    resale_partner_name,
    COALESCE(parent_crm_account_segment, 'SMB')                           AS parent_crm_account_segment,
    parent_crm_account_geo,
    parent_crm_account_region,
    parent_crm_account_country,
    CASE
      WHEN product_tier_name ILIKE '%Premium%'
        THEN 'Premium'
      WHEN product_tier_name ILIKE '%Dedicate%'
        THEN 'Dedicated-Ultimate'
      WHEN product_tier_name ILIKE '%Ultimate%'
        THEN 'Ultimate'
      WHEN product_name ILIKE '%duo%enterprise%' 
        THEN 'Duo Enterprise' 
      WHEN product_name ILIKE '%duo%pro%' 
        THEN 'Duo Pro'
      WHEN product_name ILIKE '%Enterprise%Agile%Planning%'
        THEN 'Enterprise Agile Planning'
      WHEN product_tier_name ILIKE '%Bronze%' OR product_tier_name ILIKE '%Starter%'
        THEN 'Bronze/Starter'
      ELSE 'Others'
    END                                                                   AS product_name,
    product_delivery_type,
    product_tier_name,
    rate_plan_charge_description,
    CASE
      WHEN deal_path_name IS NULL OR deal_path_name = 'Other'
        THEN 'Direct'
      ELSE deal_path_name
    END                                                                   AS deal_path_name,
    COALESCE(subscription_type, 'New Business')                           AS subscription_type,
    order_type,
    CASE
      WHEN quantity_with_addon_update <= 100 THEN 'a) 1-100'
      WHEN quantity_with_addon_update <= 250 THEN 'b) 100-250'
      WHEN quantity_with_addon_update <= 500 THEN 'c) 250-500'
      WHEN quantity_with_addon_update <= 750 THEN 'd) 500-750'
      WHEN quantity_with_addon_update <= 1500 THEN 'e) 750-1500'
      WHEN quantity_with_addon_update <= 2000 THEN 'f) 1500-2000'
      WHEN quantity_with_addon_update <= 2500 THEN 'g) 2000-2500'
      WHEN quantity_with_addon_update <= 3000 THEN 'h) 2500-3000'
      WHEN quantity_with_addon_update <= 3500 THEN 'i) 3000-3500'
      WHEN quantity_with_addon_update <= 4000 THEN 'j) 3500-4000'
      WHEN quantity_with_addon_update <= 4500 THEN 'k) 4000-4500'
      WHEN quantity_with_addon_update <= 5000 THEN 'l) 4500-5000'
      WHEN quantity_with_addon_update <= 7500 THEN 'm) 5000-7500'
      WHEN quantity_with_addon_update <= 10000 THEN 'n) 7500-10000'
      WHEN quantity_with_addon_update > 10000 THEN 'o) >10000'
    END                                                                   AS seats_bucket,
    CASE
      WHEN list_price_calc <= 5000 THEN 'a) <$5K'
      WHEN list_price_calc <= 25000 THEN 'b) $5K-$25K'
      WHEN list_price_calc <= 50000 THEN 'c) $25K-$50K'
      WHEN list_price_calc <= 100000 THEN 'd) $50K-$100K'
      WHEN list_price_calc <= 300000 THEN 'e) $100K-$300K'
      WHEN list_price_calc <= 500000 THEN 'f) $300K-$500K'
      WHEN list_price_calc <= 1000000 THEN 'g) $500K-$1M'
      WHEN list_price_calc > 1000000 THEN 'h) $1M+'
    END                                                                   AS list_price_buckets,
    CASE
      WHEN end_user_discount = 0 THEN 'a) 0%'
      WHEN end_user_discount <= 0.05 THEN 'b) <5%'
      WHEN end_user_discount <= 0.1 THEN 'c) 5-10%'
      WHEN end_user_discount <= 0.2 THEN 'd) 10-20%'
      WHEN end_user_discount <= 0.3 THEN 'e) 20-30%'
      WHEN end_user_discount <= 0.4 THEN 'f) 30-40%'
      WHEN end_user_discount <= 0.5 THEN 'g) 40-50%'
      WHEN end_user_discount <= 0.6 THEN 'h) 50-60%'
      WHEN end_user_discount <= 0.7 THEN 'i) 60-70%'
      WHEN end_user_discount <= 0.8 THEN 'j) 70-80%'
      WHEN end_user_discount <= 0.9 THEN 'k) 80-90%'
      WHEN end_user_discount > 0.9 THEN 'l) >90%'
    END                                                                   AS end_user_discount_buckets,
    SUM(quantity_with_addon_update)                                       AS quantity,
    SUM(list_price_calc)                                                  AS list_price,
    SUM(arr_calc)                                                         AS arr,
    SUM(arr_incl_channel_margin)                                          AS arr_with_channel_margin,
    (arr_with_channel_margin - arr) / NULLIFZERO(arr_with_channel_margin) AS programmatic_discount,
    (list_price - arr_with_channel_margin) / NULLIFZERO(list_price)       AS discount_end_user,
    (list_price - arr) / NULLIFZERO(list_price)                           AS discount_overall
  FROM discount_prep_step_3
  {{ dbt_utils.group_by(n=24) }}

),

manager_data AS (

  --Data for Revenue & Analytics - Discounting Historicals

  SELECT
    dim_crm_user_id,
    user_name,
    user_email,
    manager_id
  FROM dim_crm_user

),

final AS (

  --Joining Sales Analytics - Discount Analysis Dashboard and additional data for Revenue & Analytics - Discounting Historicals

  SELECT
    discount_prep_step_4.*,
    dim_crm_opportunity.opportunity_name,
    CASE
      WHEN dim_crm_user_hierarchy.crm_user_geo = 'APAC'
        THEN 'APJ'
      ELSE dim_crm_user_hierarchy.crm_user_geo
    END                                           AS report_geo_updated,
    dim_crm_user_hierarchy.crm_user_area          AS report_area,
    dim_crm_user_hierarchy.crm_user_region        AS report_region,
    dim_crm_user_hierarchy.crm_user_sales_segment AS report_segment,
    fct_crm_opportunity.dim_crm_user_id           AS opportunity_owner_id,
    fct_crm_opportunity.dim_crm_account_id,
    dim_crm_user.user_name                        AS opportunity_owner,
    dim_crm_user.user_email                       AS opportunity_owner_email,
    manager_data.user_name                        AS opportunity_owner_manager_name,
    manager_data.user_email                       AS opportunity_owner_manager_email,
    manager_data_3rd_line.user_email              AS opportunity_owner_3rd_line_manager_email,
    manager_data_4th_line.user_email              AS opportunity_owner_4th_line_manager_email
  FROM discount_prep_step_4
  LEFT JOIN dim_crm_opportunity
    ON discount_prep_step_4.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN fct_crm_opportunity
    ON dim_crm_opportunity.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_user_hierarchy
    ON fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN dim_crm_user
    ON fct_crm_opportunity.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  LEFT JOIN manager_data
    ON dim_crm_user.manager_id = manager_data.dim_crm_user_id
  LEFT JOIN manager_data AS manager_data_3rd_line
    ON manager_data.manager_id = manager_data_3rd_line.dim_crm_user_id
  LEFT JOIN manager_data AS manager_data_4th_line
    ON manager_data_3rd_line.manager_id = manager_data_4th_line.dim_crm_user_id

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-09-29",
updated_date="2025-02-27"
) }}
