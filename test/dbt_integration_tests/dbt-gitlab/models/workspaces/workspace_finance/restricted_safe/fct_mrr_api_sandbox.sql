{{ config({
    "alias": "fct_mrr_api_sandbox"
}) }}

/* grain: one record per rate_plan_charge per month */

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_charge', 'prep_charge_api_sandbox')
]) }}

, mrr AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_date.date_id','prep_charge.dim_charge_id']) }}       AS mrr_id,
      dim_date.date_id                                                                      AS dim_date_id,
      prep_charge.dim_charge_id,
      prep_charge.dim_product_detail_id,
      prep_charge.dim_subscription_id,
      prep_charge.dim_billing_account_id,
      prep_charge.dim_crm_account_id,
      prep_charge.dim_order_id,
      prep_charge.subscription_status,
      prep_charge.unit_of_measure,
      SUM(prep_charge.mrr)                                                                  AS mrr,
      SUM(prep_charge.arr)                                                                  AS arr,
      SUM(prep_charge.quantity)                                                             AS quantity
    FROM prep_charge
    INNER JOIN dim_date
      ON prep_charge.effective_start_month <= dim_date.date_actual
      AND (prep_charge.effective_end_month > dim_date.date_actual
        OR prep_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    WHERE subscription_status NOT IN ('Draft')
      AND charge_type = 'Recurring'
      /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
         Pull in seats from Paid EDU Plans with no ARR */
      AND (mrr != 0 OR LOWER(prep_charge.rate_plan_charge_name) = 'max enrollment')
    {{ dbt_utils.group_by(n=10) }}
)

{{ dbt_audit(
    cte_ref="mrr",
    created_by="@ken_aguilar",
    updated_by="@chrissharp",
    created_date="2021-09-02",
    updated_date="2023-03-29",
) }}