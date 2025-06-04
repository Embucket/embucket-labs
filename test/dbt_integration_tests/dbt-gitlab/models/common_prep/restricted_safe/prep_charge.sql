{{ simple_cte([
    ('map_merged_crm_account', 'map_merged_crm_account'),
    ('zuora_rate_plan', 'zuora_rate_plan_source'),
    ('zuora_rate_plan_charge', 'zuora_rate_plan_charge_source'),
    ('zuora_order_action_rate_plan','zuora_query_api_order_action_rate_plan_source'),
    ('zuora_order_action', 'zuora_order_action_source'),
    ('revenue_contract_line', 'zuora_revenue_revenue_contract_line_source'),
    ('zuora_order', 'zuora_order_source'),
    ('charge_contractual_value', 'zuora_query_api_charge_contractual_value_source'),
    ('booking_transaction', 'zuora_booking_transaction_source')
])}}

, sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL
    
), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
      AND LOWER(batch) != 'batch20'

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), active_zuora_subscription AS (

    SELECT *
    FROM zuora_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')
  
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
      zuora_account.account_id                                  AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                 AS dim_crm_account_id,
      MD5(rate_plan_charge_id)                                  AS dim_charge_id,
      active_zuora_subscription.subscription_id                 AS dim_subscription_id,
      active_zuora_subscription.subscription_name               AS subscription_name,
      active_zuora_subscription.subscription_status             AS subscription_status,
      product_rate_plan_charge_id                               AS dim_product_detail_id,
      true_up_lines_dates.revenue_start_date                    AS revenue_start_date,
      true_up_lines_dates.revenue_end_date                      AS revenue_end_date,
      revenue_contract_line.revenue_contract_line_created_date  AS revenue_contract_line_created_date,
      revenue_contract_line.revenue_contract_line_updated_date  AS revenue_contract_line_updated_date
    FROM revenue_contract_line
    INNER JOIN active_zuora_subscription
      ON revenue_contract_line.subscription_name = active_zuora_subscription.subscription_name
    INNER JOIN zuora_account
      ON revenue_contract_line.customer_number = zuora_account.account_number
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
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
      MIN(lns.revenue_contract_line_created_date)   AS revenue_contract_line_created_date,
      MAX(lns.revenue_contract_line_updated_date)   AS revenue_contract_line_updated_date,
      SUM(mje.adjustment)                           AS adjustment,
      MIN(revenue_start_date)                       AS revenue_start_date,
      MAX(revenue_end_date)                         AS revenue_end_date
    FROM true_up_lines lns
    LEFT JOIN mje_summed mje
      ON lns.revenue_contract_line_id = mje.revenue_contract_line_id
    WHERE adjustment IS NOT NULL
      AND ABS(ROUND(adjustment,5)) > 0
    {{ dbt_utils.group_by(n=7) }}

), charge_to_order AS (

    SELECT 
      zuora_rate_plan_charge.rate_plan_charge_id,
      zuora_order_action.order_id
    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_order_action_rate_plan
      ON zuora_rate_plan.rate_plan_id = zuora_order_action_rate_plan.rate_plan_id
    INNER JOIN zuora_order_action
      ON zuora_order_action_rate_plan.order_action_id = zuora_order_action.order_action_id
    {{ dbt_utils.group_by(n=2) }}

), non_manual_charges AS (

    SELECT
      --Natural Key
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.version                                        AS subscription_version,
      zuora_subscription.created_by_id                                  AS subscription_created_by_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.version                                    AS rate_plan_charge_version,
      zuora_rate_plan_charge.segment                                    AS rate_plan_charge_segment,

      --Surrogate Key
      zuora_rate_plan_charge.rate_plan_charge_id                        AS dim_charge_id,

      --Common Dimension Keys
      zuora_rate_plan_charge.product_rate_plan_charge_id                AS dim_product_detail_id,
      zuora_rate_plan.amendement_id                                     AS dim_amendment_id_charge,
      zuora_rate_plan.subscription_id                                   AS dim_subscription_id,
      zuora_rate_plan_charge.account_id                                 AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                         AS dim_crm_account_id,
      sfdc_account.ultimate_parent_account_id                           AS dim_parent_crm_account_id,
      charge_to_order.order_id                                          AS dim_order_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_start_date') }}  AS effective_start_date_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_end_date') }}    AS effective_end_date_id,

      --Information
      zuora_subscription.subscription_status                            AS subscription_status,
      zuora_rate_plan.rate_plan_name                                    AS rate_plan_name,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.description                                AS rate_plan_charge_description,
      zuora_rate_plan_charge.is_last_segment,
      zuora_rate_plan_charge.discount_level,
      zuora_rate_plan_charge.charge_type,
      zuora_rate_plan.amendement_type                                   AS rate_plan_charge_amendement_type,
      zuora_rate_plan_charge.unit_of_measure,
      CASE
        WHEN DATE_TRUNC('month',zuora_rate_plan_charge.charged_through_date) = zuora_rate_plan_charge.effective_end_month::DATE
          THEN TRUE ELSE FALSE
      END                                                               AS is_paid_in_full,
      CASE
        WHEN charged_through_date IS NULL THEN zuora_subscription.current_term
        ELSE DATEDIFF('month',DATE_TRUNC('month', zuora_rate_plan_charge.charged_through_date::DATE), zuora_rate_plan_charge.effective_end_month::DATE)
      END                                                               AS months_of_future_billings,
      CASE
        WHEN effective_end_month > effective_start_month OR effective_end_month IS NULL
          THEN TRUE
        ELSE FALSE
      END                                                               AS is_included_in_arr_calc,

      --Dates
      zuora_subscription.subscription_start_date                        AS subscription_start_date,
      zuora_subscription.subscription_end_date                          AS subscription_end_date,
      zuora_rate_plan_charge.effective_start_date::DATE                 AS effective_start_date,
      zuora_rate_plan_charge.effective_end_date::DATE                   AS effective_end_date,
      zuora_rate_plan_charge.effective_start_month::DATE                AS effective_start_month,
      zuora_rate_plan_charge.effective_end_month::DATE                  AS effective_end_month,
      zuora_rate_plan_charge.charged_through_date::DATE                 AS charged_through_date,
      zuora_rate_plan_charge.created_date::DATE                         AS charge_created_date,
      zuora_rate_plan_charge.updated_date::DATE                         AS charge_updated_date,
      DATEDIFF(month, zuora_rate_plan_charge.effective_start_month::DATE, zuora_rate_plan_charge.effective_end_month::DATE)
                                                                        AS charge_term,
      zuora_rate_plan_charge.billing_period,
      zuora_rate_plan_charge.specific_billing_period,

      --Additive Fields
      zuora_rate_plan_charge.mrr,
      LAG(zuora_rate_plan_charge.mrr,1) OVER (PARTITION BY zuora_subscription.subscription_name, zuora_rate_plan_charge.rate_plan_charge_number
                                              ORDER BY zuora_rate_plan_charge.segment, zuora_subscription.version)
                                                                        AS previous_mrr_calc,
      CASE
        WHEN previous_mrr_calc IS NULL
          THEN 0 ELSE previous_mrr_calc
      END                                                               AS previous_mrr,
      zuora_rate_plan_charge.mrr - previous_mrr                         AS delta_mrr_calc,
      CASE
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
          THEN -previous_mrr
        WHEN LOWER(subscription_status) = 'cancelled' AND is_last_segment = TRUE
          THEN -previous_mrr
        ELSE delta_mrr_calc
      END                                                               AS delta_mrr,
      zuora_rate_plan_charge.delta_mrc,
      zuora_rate_plan_charge.mrr * 12                                   AS arr,
      previous_mrr * 12                                                 AS previous_arr,
      zuora_rate_plan_charge.delta_mrc * 12                             AS delta_arc,
      delta_mrr * 12                                                    AS delta_arr,
      booking_transaction.list_price,
      charge_contractual_value.elp                                      AS extended_list_price,
      zuora_rate_plan_charge.quantity,
      LAG(zuora_rate_plan_charge.quantity,1) OVER (PARTITION BY zuora_subscription.subscription_name, zuora_rate_plan_charge.rate_plan_charge_number
                                                   ORDER BY zuora_rate_plan_charge.segment, zuora_subscription.version)
                                                                        AS previous_quantity_calc,
      CASE
        WHEN previous_quantity_calc IS NULL
          THEN 0 ELSE previous_quantity_calc
      END                                                               AS previous_quantity,
      zuora_rate_plan_charge.quantity - previous_quantity               AS delta_quantity_calc,
      CASE
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
          THEN -previous_quantity
        WHEN LOWER(subscription_status) = 'cancelled' AND is_last_segment = TRUE
          THEN -previous_quantity
        ELSE delta_quantity_calc
      END                                                               AS delta_quantity,
      zuora_rate_plan_charge.tcv,
      zuora_rate_plan_charge.delta_tcv,
      CASE
        WHEN is_paid_in_full = FALSE THEN months_of_future_billings * zuora_rate_plan_charge.mrr
        ELSE 0
      END                                                               AS estimated_total_future_billings,
      0                                                                 AS is_manual_charge

    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_subscription
      ON zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_account
      ON map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id
    LEFT JOIN charge_to_order
      ON zuora_rate_plan_charge.rate_plan_charge_id = charge_to_order.rate_plan_charge_id
    LEFT JOIN charge_contractual_value
      ON charge_contractual_value.rate_plan_charge_id = zuora_rate_plan_charge.rate_plan_charge_id
    LEFT JOIN booking_transaction
      ON booking_transaction.rate_plan_charge_id = zuora_rate_plan_charge.rate_plan_charge_id

 ), manual_charges_prep AS (
  
    SELECT 
      dim_billing_account_id,
      dim_crm_account_id,
      dim_charge_id,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      dim_product_detail_id,
      revenue_contract_line_created_date,
      revenue_contract_line_updated_date,
      adjustment/ROUND(MONTHS_BETWEEN(revenue_end_date::date, revenue_start_date::date),0)  AS mrr,
      NULL                                                                                  AS delta_tcv,
      'Seats'                                                                               AS unit_of_measure,
      0                                                                                     AS quantity,
      revenue_start_date::DATE                                                              AS effective_start_date,
      DATEADD('day',1,revenue_end_date::DATE)                                               AS effective_end_date
    FROM true_up_lines_subcription_grain

), manual_charges AS ( 

    SELECT
      active_zuora_subscription.subscription_name                                           AS subscription_name,
      active_zuora_subscription.subscription_name_slugify                                   AS subscription_name_slugify,
      active_zuora_subscription.version                                                     AS subscription_version,
      active_zuora_subscription.created_by_id                                               AS subscription_created_by_id,
      NULL                                                                                  AS rate_plan_charge_number,
      NULL                                                                                  AS rate_plan_charge_version,
      NULL                                                                                  AS rate_plan_charge_segment,
      manual_charges_prep.dim_charge_id                                                     AS dim_charge_id,
      manual_charges_prep.dim_product_detail_id                                             AS dim_product_detail_id,
      NULL                                                                                  AS dim_amendment_id_charge,
      active_zuora_subscription.subscription_id                                             AS dim_subscription_id,
      manual_charges_prep.dim_billing_account_id                                            AS dim_billing_account_id,
      zuora_account.crm_id                                                                  AS dim_crm_account_id,
      sfdc_account.ultimate_parent_account_id                                               AS dim_parent_crm_account_id,
      MD5(-1)                                                                               AS dim_order_id,
      {{ get_date_id('manual_charges_prep.effective_start_date') }}                         AS effective_start_date_id,
      {{ get_date_id('manual_charges_prep.effective_end_date') }}                           AS effective_end_date_id,
      active_zuora_subscription.subscription_status                                         AS subscription_status,
      'manual true up allocation'                                                           AS rate_plan_name,
      'manual true up allocation'                                                           AS rate_plan_charge_name,
      'manual true up allocation'                                                           AS rate_plan_charge_description,
      'TRUE'                                                                                AS is_last_segment,
      NULL                                                                                  AS discount_level,
      'Recurring'                                                                           AS charge_type,
      NULL                                                                                  AS rate_plan_charge_amendement_type,
      manual_charges_prep.unit_of_measure                                                   AS unit_of_measure,
      'TRUE'                                                                                AS is_paid_in_full,
      active_zuora_subscription.current_term                                                AS months_of_future_billings,
      CASE
        WHEN DATE_TRUNC('month', effective_end_date) > DATE_TRUNC('month', effective_start_date) OR DATE_TRUNC('month', effective_end_date) IS NULL
          THEN TRUE
        ELSE FALSE
      END                                                                                   AS is_included_in_arr_calc,
      active_zuora_subscription.subscription_start_date                                     AS subscription_start_date,
      active_zuora_subscription.subscription_end_date                                       AS subscription_end_date,
      effective_start_date                                                                  AS effective_start_date,
      effective_end_date                                                                    AS effective_end_date,
      DATE_TRUNC('month', effective_start_date)                                             AS effective_start_month,
      DATE_TRUNC('month', effective_end_date)                                               AS effective_end_month,
      DATEADD('day',1,effective_end_date)                                                   AS charged_through_date,
      revenue_contract_line_created_date                                                    AS charge_created_date,
      revenue_contract_line_updated_date                                                    AS charge_updated_date,
      DATEDIFF('month', effective_start_month::DATE, effective_end_month::DATE)             AS charge_term,
      NULL                                                                                  AS billing_period,
      NULL                                                                                  AS specific_billing_period,
      manual_charges_prep.mrr                                                               AS mrr,
      NULL                                                                                  AS previous_mrr_calc,
      NULL                                                                                  AS previous_mrr,
      NULL                                                                                  AS delta_mrr_calc,
      NULL                                                                                  AS delta_mrr,
      NULL                                                                                  AS delta_mrc,
      manual_charges_prep.mrr * 12                                                          AS arr,
      NULL                                                                                  AS previous_arr,
      NULL                                                                                  AS delta_arc,
      NULL                                                                                  AS delta_arr,
      NULL                                                                                  AS list_price,
      NULL                                                                                  AS extended_list_price,
      0                                                                                     AS quantity,
      NULL                                                                                  AS previous_quantity_calc,
      NULL                                                                                  AS previous_quantity,
      NULL                                                                                  AS delta_quantity_calc,
      NULL                                                                                  AS delta_quantity,
      NULL                                                                                  AS tcv,
      NULL                                                                                  AS delta_tcv,
      CASE
        WHEN is_paid_in_full = FALSE THEN months_of_future_billings * manual_charges_prep.mrr
        ELSE 0
      END                                                                                   AS estimated_total_future_billings,
      1                                                                                     AS is_manual_charge

    FROM manual_charges_prep
    INNER JOIN active_zuora_subscription
      ON manual_charges_prep.subscription_name = active_zuora_subscription.subscription_name
    INNER JOIN zuora_account
      ON active_zuora_subscription.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_account
      ON map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id


), combined_charges AS (

    SELECT *
    FROM non_manual_charges

    UNION

    SELECT *
    FROM manual_charges

), arr_analysis_framework AS (

    SELECT
      combined_charges.*,
      CASE
        WHEN subscription_version = 1
          THEN 'New'
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE
          THEN 'Churn'
        WHEN LOWER(subscription_status) = 'cancelled'
          THEN 'Churn'
        WHEN arr < previous_arr AND arr > 0
          THEN 'Contraction'
        WHEN arr > previous_arr AND subscription_version > 1
          THEN 'Expansion'
        WHEN arr = previous_arr
          THEN 'No Impact'
        ELSE NULL
      END                 AS type_of_arr_change
    FROM combined_charges

)

{{ dbt_audit(
    cte_ref="arr_analysis_framework",
    created_by="@iweeks",
    updated_by="@apiaseczna",
    created_date="2021-04-28",
    updated_date="2024-07-18"
) }}
