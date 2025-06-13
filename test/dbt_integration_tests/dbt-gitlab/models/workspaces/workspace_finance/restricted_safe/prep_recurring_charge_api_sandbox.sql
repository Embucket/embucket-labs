{{ config(
    tags=["mnpi"]
) }}

/* grain: one record per subscription per month */
WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), zuora_api_sandbox_account AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_account_source') }}
    WHERE is_deleted = FALSE
    --Keep the Batch20 test accounts since they would be in scope for this sandbox model.
      --AND LOWER(batch) != 'batch20'

), zuora_api_sandbox_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_rate_plan_source') }}

), zuora_api_sandbox_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_rate_plan_charge_source') }}
    WHERE charge_type = 'Recurring'

), zuora_api_sandbox_subscription AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')
      AND subscription_status NOT IN ('Draft')

), active_zuora_subscription AS (

    SELECT *
    FROM zuora_api_sandbox_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')

), manual_arr_true_up_allocation AS (

    SELECT *
    FROM {{ ref('sheetload_manual_arr_true_up_allocation_source') }}

), manual_charges AS ( -- added as a work around until there is an automated method for adding true-up adjustments to Zuora Revenue/Zuora Billing

    SELECT
      manual_arr_true_up_allocation.account_id                                    AS billing_account_id,
      map_merged_crm_account.dim_crm_account_id                                   AS crm_account_id,
      MD5(manual_arr_true_up_allocation.rate_plan_charge_id)                      AS rate_plan_charge_id,
      active_zuora_subscription.subscription_id                                   AS subscription_id,
      active_zuora_subscription.subscription_name                                 AS subscription_name,
      active_zuora_subscription.subscription_status                               AS subscription_status,
      manual_arr_true_up_allocation.dim_product_detail_id                         AS product_details_id,
      manual_arr_true_up_allocation.mrr                                           AS mrr,
      NULL                                                                        AS delta_tcv,
      manual_arr_true_up_allocation.unit_of_measure                               AS unit_of_measure,
      0                                                                           AS quantity,
      DATE_TRUNC('month', effective_start_date)                                   AS effective_start_month,
      DATE_TRUNC('month', effective_end_date)                                     AS effective_end_month
    FROM manual_arr_true_up_allocation
    INNER JOIN active_zuora_subscription
      ON manual_arr_true_up_allocation.subscription_name = active_zuora_subscription.subscription_name
    INNER JOIN zuora_api_sandbox_account
      ON active_zuora_subscription.account_id = zuora_api_sandbox_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_api_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

), rate_plan_charge_filtered AS (

    SELECT
      zuora_api_sandbox_account.account_id                                        AS billing_account_id,
      map_merged_crm_account.dim_crm_account_id                                   AS crm_account_id,
      zuora_api_sandbox_rate_plan_charge.rate_plan_charge_id,
      zuora_api_sandbox_subscription.subscription_id,
      zuora_api_sandbox_subscription.subscription_name,
      zuora_api_sandbox_subscription.subscription_status,
      zuora_api_sandbox_rate_plan_charge.product_rate_plan_charge_id              AS product_details_id,
      zuora_api_sandbox_rate_plan_charge.mrr,
      zuora_api_sandbox_rate_plan_charge.delta_tcv,
      zuora_api_sandbox_rate_plan_charge.unit_of_measure,
      zuora_api_sandbox_rate_plan_charge.quantity,
      zuora_api_sandbox_rate_plan_charge.effective_start_month,
      zuora_api_sandbox_rate_plan_charge.effective_end_month
    FROM zuora_api_sandbox_rate_plan_charge
    INNER JOIN zuora_api_sandbox_rate_plan
      ON zuora_api_sandbox_rate_plan.rate_plan_id = zuora_api_sandbox_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_api_sandbox_subscription
      ON zuora_api_sandbox_rate_plan.subscription_id = zuora_api_sandbox_subscription.subscription_id
    INNER JOIN zuora_api_sandbox_account
      ON zuora_api_sandbox_account.account_id = zuora_api_sandbox_subscription.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_api_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

), combined_rate_plans AS (

    SELECT *
    FROM rate_plan_charge_filtered

    UNION

    SELECT *
    FROM manual_charges

), mrr_month_by_month AS (

    SELECT
      dim_date.date_id,
      billing_account_id,
      crm_account_id,
      subscription_id,
      subscription_name,
      subscription_status,
      product_details_id,
      rate_plan_charge_id,
      SUM(mrr)                                                           AS mrr,
      SUM(mrr)* 12                                                       AS arr,
      SUM(quantity)                                                      AS quantity,
      ARRAY_AGG(combined_rate_plans.unit_of_measure)                     AS unit_of_measure
    FROM combined_rate_plans
    INNER JOIN dim_date
      ON combined_rate_plans.effective_start_month <= dim_date.date_actual
      AND (combined_rate_plans.effective_end_month > dim_date.date_actual
        OR combined_rate_plans.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    {{ dbt_utils.group_by(n=8) }}

), final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['date_id','rate_plan_charge_id']) }}     AS mrr_id,
    date_id                                                              AS dim_date_id,
    billing_account_id                                                   AS dim_billing_account_id,
    crm_account_id                                                       AS dim_crm_account_id,
    subscription_id                                                      AS dim_subscription_id,
    product_details_id                                                   AS dim_product_detail_id,
    rate_plan_charge_id                                                  AS dim_charge_id,
    subscription_status,
    mrr,
    arr,
    quantity,
    unit_of_measure
  FROM mrr_month_by_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2021-09-02",
    updated_date="2021-09-02",
) }}
