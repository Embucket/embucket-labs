 {{ config({
        "materialized": "table",
        "transient": false,
        "alias": "mart_arr_all"
    })
}} 

{{ mart_arr_with_zero_dollar_charges_source('fct_mrr_with_zero_dollar_charges') }}
,
joined_with_arr_month AS (

  SELECT
  -- Primary Key
        {{ dbt_utils.generate_surrogate_key(['joined.dim_date_id', 
                                            'joined.subscription_name', 
                                            'joined.dim_product_detail_id', 
                                            'joined.dim_charge_id']) }}
                                                                                      AS primary_key,
    joined.*,
    joined.date_actual AS arr_month
  FROM joined
),

cohort_diffs AS (

  SELECT
    joined_with_arr_month.*,
    DATEDIFF(MONTH, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    DATEDIFF(QUARTER, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    DATEDIFF(MONTH, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    DATEDIFF(QUARTER, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    DATEDIFF(MONTH, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    DATEDIFF(QUARTER, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    DATEDIFF(MONTH, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    DATEDIFF(QUARTER, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined_with_arr_month

),

parent_arr AS (

  SELECT
    arr_month,
    dim_parent_crm_account_id,
    SUM(arr) AS arr
  FROM joined_with_arr_month
  {{ dbt_utils.group_by(n=2) }}

),

parent_arr_band_calc AS (

  SELECT
    arr_month,
    dim_parent_crm_account_id,
    CASE
      WHEN arr > 5000 THEN 'ARR > $5K'
      WHEN arr <= 5000 THEN 'ARR <= $5K'
    END AS arr_band_calc
  FROM parent_arr

),

final_table AS (

  SELECT
    cohort_diffs.dim_charge_id,
    --Date info
    cohort_diffs.arr_month,
    cohort_diffs.fiscal_quarter_name_fy,
    cohort_diffs.fiscal_year,
    {{ mart_arr_with_zero_dollar_charges_fields('cohort_diffs') }},
    cohort_diffs.months_since_billing_account_cohort_start,
    cohort_diffs.quarters_since_billing_account_cohort_start,
    cohort_diffs.months_since_crm_account_cohort_start,
    cohort_diffs.quarters_since_crm_account_cohort_start,
    cohort_diffs.months_since_parent_account_cohort_start,
    cohort_diffs.quarters_since_parent_account_cohort_start,
    cohort_diffs.months_since_subscription_cohort_start,
    cohort_diffs.quarters_since_subscription_cohort_start,
    parent_arr_band_calc.arr_band_calc
  FROM cohort_diffs
  LEFT JOIN parent_arr_band_calc
    ON cohort_diffs.arr_month = parent_arr_band_calc.arr_month
      AND cohort_diffs.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@snalamaru",
    updated_by="@rakhireddy",
    created_date="2023-12-01",
    updated_date="2025-03-07"
) }}
