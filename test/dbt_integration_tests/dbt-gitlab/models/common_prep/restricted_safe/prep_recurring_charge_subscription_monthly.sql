/* grain: one record per subscription per month */
{{ simple_cte([
    ('zuora_rate_plan', 'zuora_rate_plan_source'),
    ('map_merged_crm_account', 'map_merged_crm_account'),
    ('product_details', 'dim_product_detail'),
    ('dim_date', 'dim_date'),
]) }}

, zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
      AND LOWER(batch) != 'batch20'

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}
    WHERE charge_type = 'Recurring'

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), rate_plan_charge_filtered AS (

    SELECT
      zuora_account.account_id                                      AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                     AS dim_crm_account_id,
      zuora_subscription.subscription_id                            AS dim_subscription_id,
      zuora_subscription.original_id                                AS dim_subscription_id_original,
      zuora_subscription.subscription_status,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.effective_start_month,
      zuora_rate_plan_charge.effective_end_month,
      product_details.product_delivery_type
    FROM zuora_rate_plan_charge
    INNER JOIN zuora_rate_plan
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_subscription
      ON zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_account
      ON zuora_account.account_id = zuora_subscription.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN product_details
      ON zuora_rate_plan_charge.product_rate_plan_charge_id = product_details.dim_product_detail_id

), mrr_by_delivery_type AS (

  SELECT
      dim_date.date_id                                                  AS dim_date_id,
      dim_date.first_day_of_month                                       AS charge_month,
      dim_billing_account_id,
      dim_crm_account_id,
      dim_subscription_id,
      dim_subscription_id_original,
      subscription_status,
      product_delivery_type,
      unit_of_measure,
      {{ dbt_utils.generate_surrogate_key(['dim_date_id',
                                  'dim_subscription_id',
                                  'product_delivery_type',
                                  'unit_of_measure']) }}                AS mrr_id,
      SUM(mrr)                                                          AS mrr,
      SUM(mrr) * 12                                                     AS arr,
      SUM(quantity)                                                     AS quantity
    FROM rate_plan_charge_filtered
    INNER JOIN dim_date
      ON rate_plan_charge_filtered.effective_start_month <= dim_date.date_actual
      AND (rate_plan_charge_filtered.effective_end_month > dim_date.date_actual
           OR rate_plan_charge_filtered.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    {{ dbt_utils.group_by(n=10) }}

), mrr_by_subscription AS (

  SELECT
      subscription.dim_billing_account_id,
      subscription.dim_crm_account_id,
      subscription.dim_subscription_id,
      subscription.dim_subscription_id_original,
      subscription.subscription_status,
      subscription.dim_date_id,
      subscription.charge_month,
      SUM(sm.mrr)                                                       AS sm_mrr,
      SUM(sm.arr)                                                       AS sm_arr,
      SUM(sm.quantity)                                                  AS sm_quantity,
      SUM(saas.mrr)                                                     AS saas_mrr,
      SUM(saas.arr)                                                     AS saas_arr,
      SUM(saas.quantity)                                                AS saas_quantity,
      SUM(other.mrr)                                                    AS other_mrr,
      SUM(other.arr)                                                    AS other_arr,
      SUM(other.quantity)                                               AS other_quantity,
      SUM(subscription.mrr)                                             AS total_mrr,
      SUM(subscription.arr)                                             AS total_arr,
      SUM(subscription.quantity)                                        AS total_quantity,
      ARRAY_AGG(subscription.product_delivery_type
                || ': '
                || subscription.unit_of_measure)
        WITHIN GROUP (ORDER BY subscription.product_delivery_type DESC) AS unit_of_measure
    FROM mrr_by_delivery_type subscription
    LEFT JOIN mrr_by_delivery_type sm
      ON sm.product_delivery_type = 'Self-Managed'
      AND subscription.mrr_id = sm.mrr_id
    LEFT JOIN mrr_by_delivery_type saas
      ON saas.product_delivery_type = 'SaaS'
      AND subscription.mrr_id = saas.mrr_id
    LEFT JOIN mrr_by_delivery_type other
      ON other.product_delivery_type = 'Others'
      AND subscription.mrr_id = other.mrr_id
    {{ dbt_utils.group_by(n=7) }}

), final AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      dim_crm_account_id,
      dim_date_id,
      charge_month,
      subscription_status,
      unit_of_measure,
      total_mrr,
      total_arr,
      total_quantity,
      sm_mrr,
      sm_arr,
      sm_quantity,
      saas_mrr,
      saas_arr,
      saas_quantity,
      other_mrr,
      other_arr,
      other_quantity,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY dim_subscription_id
            ORDER BY dim_date_id DESC) = 1,
          TRUE, FALSE)                                                      AS is_latest_record_per_subscription
    FROM mrr_by_subscription

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@iweeks",
    created_date="2021-03-01",
    updated_date="2021-07-29"
) }}
