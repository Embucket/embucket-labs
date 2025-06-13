{{ config({
    "alias": "fct_mrr_all_weekly"
}) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_charge', 'prep_charge'),
    ('dim_crm_account', 'dim_crm_account')
]) }}

, prep_charge_filtered AS (

    SELECT 
      prep_charge.*,
      dim_crm_account.is_jihu_account
    FROM prep_charge
    LEFT JOIN dim_crm_account
      ON prep_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE subscription_status NOT IN ('Draft')
      AND charge_type = 'Recurring'
      AND is_included_in_arr_calc = TRUE


), dim_date_weekly AS (

    SELECT DISTINCT
      first_day_of_week
    FROM dim_date


), mrr_daily AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_date.date_id','prep_charge_filtered.dim_charge_id'])}}          AS mrr_id,
      dim_date.date_id                                                                                          AS dim_date_id,
      dim_date.date_actual                                                                                      AS date_actual,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL)         AS fiscal_quarter_name_fy,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)                       AS fiscal_year,
      prep_charge_filtered.dim_charge_id AS dim_charge_id,
      prep_charge_filtered.dim_product_detail_id AS dim_product_detail_id,
      prep_charge_filtered.dim_subscription_id AS dim_subscription_id,
      prep_charge_filtered.dim_billing_account_id AS dim_billing_account_id,
      prep_charge_filtered.dim_crm_account_id AS dim_crm_account_id,
      prep_charge_filtered.dim_order_id AS dim_order_id,
      prep_charge_filtered.subscription_status AS subscription_status,
      prep_charge_filtered.unit_of_measure AS unit_of_measure,
      prep_charge_filtered.is_jihu_account AS is_jihu_account,
      SUM(prep_charge_filtered.mrr)                                                                             AS mrr,
      SUM(prep_charge_filtered.arr)                                                                             AS arr,
      SUM(prep_charge_filtered.quantity)                                                                        AS quantity
    FROM prep_charge_filtered
    INNER JOIN dim_date
      ON prep_charge_filtered.effective_start_date <= dim_date.date_actual
      AND (prep_charge_filtered.effective_end_date > dim_date.date_actual
        OR prep_charge_filtered.effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=14) }}


), mrr_weekly AS (

    SELECT
      mrr_daily.mrr_id,
      mrr_daily.dim_date_id,
      mrr_daily.date_actual,
      mrr_daily.fiscal_quarter_name_fy,
      mrr_daily.fiscal_year,
      mrr_daily.dim_charge_id,
      mrr_daily.dim_product_detail_id,
      mrr_daily.dim_subscription_id,
      mrr_daily.dim_billing_account_id,
      mrr_daily.dim_crm_account_id,
      mrr_daily.dim_order_id,
      mrr_daily.subscription_status,
      mrr_daily.unit_of_measure,
      mrr_daily.is_jihu_account,
      mrr_daily.mrr,
      mrr_daily.arr,
      mrr_daily.quantity
    FROM mrr_daily
    INNER JOIN dim_date_weekly
      ON mrr_daily.date_actual = dim_date_weekly.first_day_of_week

)

SELECT *
FROM mrr_weekly
