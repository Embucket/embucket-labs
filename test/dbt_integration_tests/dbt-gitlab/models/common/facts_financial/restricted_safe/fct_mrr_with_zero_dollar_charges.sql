{{ config({
    "alias": "fct_mrr_all"
}) }}

/* grain: one record per rate_plan_charge per month */

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_charge', 'prep_charge'),
    ('dim_crm_account', 'dim_crm_account')
]) }}

, mrr AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_date.date_id','prep_charge.dim_charge_id']) }}       AS mrr_id,
      dim_date.date_id                                                                      AS dim_date_id,
      dim_date.date_actual,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)               AS fiscal_year, 
      prep_charge.dim_charge_id,
      prep_charge.dim_product_detail_id,
      prep_charge.dim_subscription_id,
      prep_charge.dim_billing_account_id,
      prep_charge.dim_crm_account_id,
      prep_charge.dim_order_id,
      prep_charge.subscription_status,
      prep_charge.unit_of_measure,
      dim_crm_account.is_jihu_account,
      SUM(prep_charge.mrr)                                                                  AS mrr,
      SUM(prep_charge.arr)                                                                  AS arr,
      SUM(prep_charge.quantity)                                                             AS quantity,
    FROM prep_charge
    LEFT JOIN dim_crm_account
      ON prep_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    INNER JOIN dim_date
      ON prep_charge.effective_start_month <= dim_date.date_actual
      AND (prep_charge.effective_end_month > dim_date.date_actual
        OR prep_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    WHERE subscription_status NOT IN ('Draft')
      AND charge_type = 'Recurring'
    {{ dbt_utils.group_by(n=14) }}
)

{{ dbt_audit(
    cte_ref="mrr",
    created_by="@iweeks",
    updated_by="@rakhireddy",
    created_date="2022-04-04",
    updated_date="2025-02-10",
) }}
