/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{ config({
        "materialized": "table",
        "transient": false
    })
}}

WITH dim_billing_account AS (

  SELECT *
  FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

), dim_product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}

), dim_subscription AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}

), fct_mrr AS (

  SELECT
    dim_date_id,
    dim_subscription_id,
    dim_product_detail_id,
    dim_billing_account_id,
    dim_crm_account_id,
    dim_charge_id,
    SUM(mrr)                                                                      AS mrr,
    SUM(arr)                                                                      AS arr,
    SUM(quantity)                                                                 AS quantity,
    ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure)   AS unit_of_measure
  FROM {{ ref('fct_mrr') }}
  WHERE subscription_status IN ('Active', 'Cancelled')
  {{ dbt_utils.group_by(n=6) }}

), joined AS (

    SELECT
      --primary_key
      {{ dbt_utils.generate_surrogate_key(['fct_mrr.dim_date_id', 
                                           'dim_subscription.subscription_name', 
                                           'fct_mrr.dim_product_detail_id',
                                           'fct_mrr.dim_charge_id']) }}
                                                                                      AS primary_key,

      --date info
      dim_date.date_actual                                                            AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS fiscal_year,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_billing_account.ssp_channel                                                 AS ssp_channel,
      dim_billing_account.po_required                                                 AS po_required,
      dim_billing_account.auto_pay                                                    AS auto_pay,
      dim_billing_account.default_payment_method_type                                 AS default_payment_method_type,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                         AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment                                AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_sales_segment_legacy                         AS parent_crm_account_sales_segment_legacy,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.crm_account_employee_count_band                                 AS crm_account_employee_count_band,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,
      dim_crm_account.is_jihu_account                                                 AS is_jihu_account,
      dim_crm_account.parent_crm_account_lam                                          AS parent_crm_account_lam,
      dim_crm_account.parent_crm_account_lam_dev_count                                AS parent_crm_account_lam_dev_count,
      dim_crm_account.parent_crm_account_business_unit                                AS parent_crm_account_business_unit,
      dim_crm_account.parent_crm_account_geo                                          AS parent_crm_account_geo,
      dim_crm_account.parent_crm_account_region                                       AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                         AS parent_crm_account_area,
      dim_crm_account.parent_crm_account_role_type                                    AS parent_crm_account_role_type,
      dim_crm_account.parent_crm_account_territory                                    AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_max_family_employee                          AS parent_crm_account_max_family_employee,
      dim_crm_account.parent_crm_account_upa_country                                  AS parent_crm_account_upa_country,
      dim_crm_account.parent_crm_account_upa_state                                    AS parent_crm_account_upa_state,
      dim_crm_account.parent_crm_account_upa_city                                     AS parent_crm_account_upa_city,
      dim_crm_account.parent_crm_account_upa_street                                   AS parent_crm_account_upa_street,
      dim_crm_account.parent_crm_account_upa_postal_code                              AS parent_crm_account_upa_postal_code,
      dim_crm_account.crm_account_employee_count                                      AS crm_account_employee_count,

      --subscription info
      dim_subscription.dim_subscription_id                                            AS dim_subscription_id,
      dim_subscription.dim_subscription_id_original                                   AS dim_subscription_id_original,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name                                              AS subscription_name,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      dim_billing_account.billing_account_arr_cohort_month                            AS billing_account_cohort_month,
      dim_billing_account.billing_account_arr_cohort_quarter                          AS billing_account_cohort_quarter,
      dim_crm_account.crm_account_arr_cohort_month                                    AS crm_account_cohort_month,
      dim_crm_account.crm_account_arr_cohort_quarter                                  AS crm_account_cohort_quarter,
      dim_crm_account.parent_account_arr_cohort_month                                 AS parent_account_cohort_month,
      dim_crm_account.parent_account_arr_cohort_quarter                               AS parent_account_cohort_quarter,
      dim_subscription.auto_renew_native_hist,
      dim_subscription.auto_renew_customerdot_hist,
      dim_subscription.turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics,
      dim_subscription.contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation,
      dim_subscription.invoice_owner_account,
      dim_subscription.creator_account,
      dim_subscription.was_purchased_through_reseller,

      --product info
      dim_product_detail.dim_product_detail_id                                        AS dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.product_deployment_type                                      AS product_deployment_type,
      dim_product_detail.product_category                                             AS product_category,
      dim_product_detail.product_rate_plan_category                                   AS product_rate_plan_category,
      dim_product_detail.product_ranking                                              AS product_ranking,          
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_charge_name                                AS product_rate_plan_charge_name,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,
      dim_product_detail.is_licensed_user                                             AS is_licensed_user,
      dim_product_detail.is_licensed_user_base_product                                AS is_licensed_user_base_product,
      dim_product_detail.is_licensed_user_add_on                                      AS is_licensed_user_add_on,
      dim_product_detail.is_arpu                                                      AS is_arpu,

      --charge info
      fct_mrr.dim_charge_id,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr.unit_of_measure                                                         AS unit_of_measure,
      fct_mrr.mrr                                                                     AS mrr,
      fct_mrr.arr                                                                     AS arr,
      fct_mrr.quantity                                                                AS quantity
    FROM fct_mrr
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE dim_crm_account.is_jihu_account != 'TRUE'

), cohort_diffs AS (

  SELECT
    joined.*,
    datediff(month, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    datediff(quarter, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    datediff(month, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    datediff(quarter, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    datediff(month, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    datediff(quarter, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    datediff(month, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    datediff(quarter, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined

), parent_arr AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                   AS arr
    FROM joined
    {{ dbt_utils.group_by(n=2) }}

), parent_arr_band_calc AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                        AS arr_band_calc
    FROM parent_arr

), final_table AS (

    SELECT
      cohort_diffs.*,
      arr_band_calc
    FROM cohort_diffs
    LEFT JOIN parent_arr_band_calc
      ON cohort_diffs.arr_month = parent_arr_band_calc.arr_month
      AND cohort_diffs.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@msendal",
    updated_by="@mdrussell",
    created_date="2020-09-04",
    updated_date="2024-12-17"
) }}
