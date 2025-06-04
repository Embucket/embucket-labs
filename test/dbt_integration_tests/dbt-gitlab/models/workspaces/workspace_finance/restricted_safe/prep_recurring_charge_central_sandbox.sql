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

), zuora_central_sandbox_account AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_account_source') }}
    WHERE is_deleted = FALSE
    --Keep the Batch20 test accounts since they would be in scope for this sandbox model.
      --AND LOWER(batch) != 'batch20'

), zuora_central_sandbox_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_rate_plan_source') }}

), zuora_central_sandbox_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_rate_plan_charge_source') }}
    WHERE charge_type = 'Recurring'

), zuora_central_sandbox_subscription AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')
      AND subscription_status NOT IN ('Draft')

), active_zuora_subscription AS (

    SELECT *
    FROM zuora_central_sandbox_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')

), revenue_contract_line AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_line_source') }}

), mje AS (

    SELECT 
      *,
      CASE 
        WHEN debit_activity_type = 'Revenue' AND  credit_activity_type = 'Contract Liability' 
          THEN -amount           
        WHEN credit_activity_type = 'Revenue' AND  debit_activity_type = 'Contract Liability' 
          THEN amount
        ELSE amount                                                                             
      END                                                                                       AS adjustment_amount
    FROM {{ ref('zuora_revenue_manual_journal_entry_source') }}
  
), true_up_lines_dates AS (
  
    SELECT 
      subscription_name,
      revenue_contract_line_attribute_16,
      MIN(revenue_start_date)               AS revenue_start_date,
      MAX(revenue_end_date)                 AS revenue_end_date
    FROM revenue_contract_line
    GROUP BY 1,2

), true_up_lines AS (

    SELECT 
      revenue_contract_line_id,
      revenue_contract_id,
      zuora_central_sandbox_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id             AS dim_crm_account_id,
      MD5(rate_plan_charge_id)                              AS dim_charge_id,
      active_zuora_subscription.subscription_id             AS dim_subscription_id,
      active_zuora_subscription.subscription_name           AS subscription_name,
      active_zuora_subscription.subscription_status         AS subscription_status,
      product_rate_plan_charge_id                           AS dim_product_detail_id,
      true_up_lines_dates.revenue_start_date                AS revenue_start_date,
      true_up_lines_dates.revenue_end_date                  AS revenue_end_date
    FROM revenue_contract_line
    INNER JOIN active_zuora_subscription
      ON revenue_contract_line.subscription_name = active_zuora_subscription.subscription_name
    INNER JOIN zuora_central_sandbox_account
      ON revenue_contract_line.customer_number = zuora_central_sandbox_account.account_number
    LEFT JOIN map_merged_crm_account
      ON zuora_central_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN true_up_lines_dates
      ON revenue_contract_line.subscription_name = true_up_lines_dates.subscription_name
        AND revenue_contract_line.revenue_contract_line_attribute_16 = true_up_lines_dates.revenue_contract_line_attribute_16
    WHERE revenue_contract_line.revenue_contract_line_attribute_16 LIKE '%True-up ARR Allocation%'
  
), mje_summed AS (
  
    SELECT
      mje.revenue_contract_line_id,
      SUM(adjustment_amount) AS adjustment
    FROM mje
    INNER JOIN true_up_lines
      ON mje.revenue_contract_line_id = true_up_lines.revenue_contract_line_id
        AND mje.revenue_contract_id = true_up_lines.revenue_contract_id
    {{ dbt_utils.group_by(n=1) }}

), true_up_lines_subcription_grain AS (
  
    SELECT
      lns.dim_billing_account_id,
      lns.dim_crm_account_id,
      lns.dim_charge_id,
      lns.dim_subscription_id,
      lns.subscription_name,
      lns.subscription_status,
      lns.dim_product_detail_id,
      SUM(mje.adjustment)               AS adjustment,
      MIN(revenue_start_date)           AS revenue_start_date,
      MAX(revenue_end_date)             AS revenue_end_date
    FROM true_up_lines lns
    LEFT JOIN mje_summed mje
      ON lns.revenue_contract_line_id = mje.revenue_contract_line_id
    WHERE adjustment IS NOT NULL
      AND ABS(ROUND(adjustment,5)) > 0
    {{ dbt_utils.group_by(n=7) }}
  
), manual_charges AS (
  
    SELECT 
      dim_billing_account_id,
      dim_crm_account_id,
      dim_charge_id,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      dim_product_detail_id,
      adjustment/ROUND(MONTHS_BETWEEN(revenue_end_date::date, revenue_start_date::date),0)  AS mrr,
      NULL                                                                                  AS delta_tcv,
      'Seats'                                                                               AS unit_of_measure,
      0                                                                                     AS quantity,
      DATE_TRUNC('month',revenue_start_date::date)                                          AS effective_start_month,
      DATE_TRUNC('month',DATEADD('day',1,revenue_end_date::date))                           AS effective_end_month
    FROM true_up_lines_subcription_grain

), rate_plan_charge_filtered AS (

    SELECT
      zuora_central_sandbox_account.account_id                            AS billing_account_id,
      map_merged_crm_account.dim_crm_account_id           AS crm_account_id,
      zuora_central_sandbox_rate_plan_charge.rate_plan_charge_id,
      zuora_central_sandbox_subscription.subscription_id,
      zuora_central_sandbox_subscription.subscription_name,
      zuora_central_sandbox_subscription.subscription_status,
      zuora_central_sandbox_rate_plan_charge.product_rate_plan_charge_id  AS product_details_id,
      zuora_central_sandbox_rate_plan_charge.mrr,
      zuora_central_sandbox_rate_plan_charge.delta_tcv,
      zuora_central_sandbox_rate_plan_charge.unit_of_measure,
      zuora_central_sandbox_rate_plan_charge.quantity,
      zuora_central_sandbox_rate_plan_charge.effective_start_month,
      zuora_central_sandbox_rate_plan_charge.effective_end_month
    FROM zuora_central_sandbox_rate_plan_charge
    INNER JOIN zuora_central_sandbox_rate_plan
      ON zuora_central_sandbox_rate_plan.rate_plan_id = zuora_central_sandbox_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_central_sandbox_subscription
      ON zuora_central_sandbox_rate_plan.subscription_id = zuora_central_sandbox_subscription.subscription_id
    INNER JOIN zuora_central_sandbox_account
      ON zuora_central_sandbox_account.account_id = zuora_central_sandbox_subscription.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_central_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

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
      SUM(mrr)                                             AS mrr,
      SUM(mrr)* 12                                         AS arr,
      SUM(quantity)                                        AS quantity,
      ARRAY_AGG(combined_rate_plans.unit_of_measure)       AS unit_of_measure
    FROM combined_rate_plans
    INNER JOIN dim_date
      ON combined_rate_plans.effective_start_month <= dim_date.date_actual
      AND (combined_rate_plans.effective_end_month > dim_date.date_actual
        OR combined_rate_plans.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    {{ dbt_utils.group_by(n=8) }}

), final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['date_id','rate_plan_charge_id']) }}                        AS mrr_id,
    date_id                                                                                 AS dim_date_id,
    billing_account_id                                                                      AS dim_billing_account_id,
    crm_account_id                                                                          AS dim_crm_account_id,
    subscription_id                                                                         AS dim_subscription_id,
    product_details_id                                                                      AS dim_product_detail_id,
    rate_plan_charge_id                                                                     AS dim_charge_id,
    subscription_status,
    mrr,
    arr,
    quantity,
    unit_of_measure
  FROM mrr_month_by_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-03-31",
    updated_date="2022-03-31",
) }}

