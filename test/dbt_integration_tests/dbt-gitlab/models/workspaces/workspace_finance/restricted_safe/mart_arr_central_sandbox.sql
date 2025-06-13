WITH dim_billing_account_central_sandbox AS (

  SELECT *
  FROM {{ ref('dim_billing_account_central_sandbox') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

), dim_product_detail_central_sandbox AS (

  SELECT *
  FROM {{ ref('dim_product_detail_central_sandbox') }}

), dim_subscription_central_sandbox AS (

  SELECT *
  FROM {{ ref('dim_subscription_central_sandbox') }}

), fct_mrr_central_sandbox AS (

  SELECT
    dim_date_id,
    dim_subscription_id,
    dim_product_detail_id,
    dim_billing_account_id,
    dim_crm_account_id,
    SUM(mrr)                                                                            AS mrr,
    SUM(arr)                                                                            AS arr,
    SUM(quantity)                                                                       AS quantity,
    ARRAY_AGG(unit_of_measure)                                                          AS unit_of_measure
  FROM {{ ref('fct_mrr_central_sandbox') }}
  WHERE subscription_status IN ('Active', 'Cancelled')
  {{ dbt_utils.group_by(n=5) }}

), joined AS (

    SELECT
      --primary_key
      {{ dbt_utils.generate_surrogate_key(['fct_mrr_central_sandbox.dim_date_id', 'dim_subscription_central_sandbox.subscription_name', 'fct_mrr_central_sandbox.dim_product_detail_id']) }}
                                                                                        AS primary_key,

      --date info
      dim_date.date_actual                                                              AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)   AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                 AS fiscal_year,
      dim_subscription_central_sandbox.subscription_start_month                         AS subscription_start_month,
      dim_subscription_central_sandbox.subscription_end_month                           AS subscription_end_month,

      --billing account info
      dim_billing_account_central_sandbox.dim_billing_account_id                        AS dim_billing_account_id,
      dim_billing_account_central_sandbox.sold_to_country                               AS sold_to_country,
      dim_billing_account_central_sandbox.billing_account_name                          AS billing_account_name,
      dim_billing_account_central_sandbox.billing_account_number                        AS billing_account_number,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                                AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                  AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                         AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                           AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_upa_country                                    AS parent_crm_account_upa_country,
      dim_crm_account.parent_crm_account_sales_segment                                  AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry                                       AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_territory                                      AS parent_crm_account_territory,
      dim_crm_account.crm_account_employee_count_band                                   AS crm_account_employee_count_band,
      dim_crm_account.parent_crm_account_geo                                            AS parent_crm_account_geo,
      dim_crm_account.parent_crm_account_region                                         AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                           AS parent_crm_account_area,
      dim_crm_account.parent_crm_account_max_family_employee                            AS parent_crm_account_max_family_employee,
      dim_crm_account.crm_account_employee_count                                        AS crm_account_employee_count,
      dim_crm_account.health_score_color                                                AS health_score_color,
      dim_crm_account.health_number                                                     AS health_number,
      dim_crm_account.is_jihu_account                                                   AS is_jihu_account,

      --subscription info
      dim_subscription_central_sandbox.dim_subscription_id                              AS dim_subscription_id,
      dim_subscription_central_sandbox.dim_subscription_id_original                     AS dim_subscription_id_original,
      dim_subscription_central_sandbox.subscription_status                              AS subscription_status,
      dim_subscription_central_sandbox.subscription_sales_type                          AS subscription_sales_type,
      dim_subscription_central_sandbox.subscription_name                                AS subscription_name,
      dim_subscription_central_sandbox.subscription_name_slugify                        AS subscription_name_slugify,
      dim_subscription_central_sandbox.oldest_subscription_in_cohort                    AS oldest_subscription_in_cohort,
      dim_subscription_central_sandbox.subscription_lineage                             AS subscription_lineage,
      dim_subscription_central_sandbox.subscription_cohort_month                        AS subscription_cohort_month,
      dim_subscription_central_sandbox.subscription_cohort_quarter                      AS subscription_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_billing_account_central_sandbox.dim_billing_account_id)      AS billing_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_billing_account_central_sandbox.dim_billing_account_id)      AS billing_account_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                              AS crm_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                              AS crm_account_cohort_quarter,
      MIN(dim_date.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                       AS parent_account_cohort_month,
      MIN(dim_date.first_day_of_fiscal_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                       AS parent_account_cohort_quarter,
      dim_subscription_central_sandbox.auto_renew_native_hist,
      dim_subscription_central_sandbox.auto_renew_customerdot_hist,
      dim_subscription_central_sandbox.turn_on_cloud_licensing,
      dim_subscription_central_sandbox.contract_auto_renewal,
      dim_subscription_central_sandbox.turn_on_auto_renewal,
      dim_subscription_central_sandbox.contract_seat_reconciliation,
      dim_subscription_central_sandbox.turn_on_seat_reconciliation,

      --product info
      dim_product_detail_central_sandbox.product_tier_name                                  AS product_tier_name,
      dim_product_detail_central_sandbox.product_delivery_type                              AS product_delivery_type,
      dim_product_detail_central_sandbox.service_type                                       AS service_type,
      dim_product_detail_central_sandbox.product_rate_plan_name                             AS product_rate_plan_name,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr_central_sandbox.unit_of_measure                                               AS unit_of_measure,
      fct_mrr_central_sandbox.mrr                                                           AS mrr,
      fct_mrr_central_sandbox.arr                                                           AS arr,
      fct_mrr_central_sandbox.quantity                                                      AS quantity
    FROM fct_mrr_central_sandbox
    INNER JOIN dim_subscription_central_sandbox
      ON dim_subscription_central_sandbox.dim_subscription_id = fct_mrr_central_sandbox.dim_subscription_id
    INNER JOIN dim_product_detail_central_sandbox
      ON dim_product_detail_central_sandbox.dim_product_detail_id = fct_mrr_central_sandbox.dim_product_detail_id
    INNER JOIN dim_billing_account_central_sandbox
      ON dim_billing_account_central_sandbox.dim_billing_account_id = fct_mrr_central_sandbox.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr_central_sandbox.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account_central_sandbox.dim_crm_account_id = dim_crm_account.dim_crm_account_id

), cohort_diffs AS (

  SELECT
    joined.*,
    datediff(month, billing_account_cohort_month, arr_month)                            AS months_since_billing_account_cohort_start,
    datediff(quarter, billing_account_cohort_quarter, arr_month)                        AS quarters_since_billing_account_cohort_start,
    datediff(month, crm_account_cohort_month, arr_month)                                AS months_since_crm_account_cohort_start,
    datediff(quarter, crm_account_cohort_quarter, arr_month)                            AS quarters_since_crm_account_cohort_start,
    datediff(month, parent_account_cohort_month, arr_month)                             AS months_since_parent_account_cohort_start,
    datediff(quarter, parent_account_cohort_quarter, arr_month)                         AS quarters_since_parent_account_cohort_start,
    datediff(month, subscription_cohort_month, arr_month)                               AS months_since_subscription_cohort_start,
    datediff(quarter, subscription_cohort_quarter, arr_month)                           AS quarters_since_subscription_cohort_start
  FROM joined

), parent_arr AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                                                          AS arr
    FROM joined
    {{ dbt_utils.group_by(n=2) }}

), parent_arr_band_calc AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                                                               AS arr_band_calc
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
    created_by="@michellecooper",
    updated_by="@lisvinueza",
    created_date="2022-03-31",
    updated_date="2023-05-22"
) }}
