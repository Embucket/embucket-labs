WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

), zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_acct_period AS (

    SELECT *
    FROM {{ ref('zuora_accounting_period_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}
    WHERE is_deleted = FALSE

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}
    WHERE is_deleted = FALSE

), zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}
    WHERE is_deleted = FALSE

), zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}
    WHERE is_deleted = FALSE

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), base_mrr AS (

    SELECT
      --primary key
      zuora_rpc.rate_plan_charge_id,

      --account info
      zuora_accts.account_id,
      zuora_accts.account_name,
      zuora_accts.account_number,
      zuora_accts.crm_id,
      zuora_contact.country,
      zuora_accts.currency,

      --subscription info
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,

      --rate_plan info
      zuora_rpc.product_rate_plan_charge_id,
      zuora_rp.rate_plan_name,
      zuora_rpc.rate_plan_charge_name,
      zuora_rpc.rate_plan_charge_number,
      zuora_rpc.unit_of_measure,
      zuora_rpc.quantity,
      zuora_rpc.mrr,
      zuora_rpc.charge_type,

      --date info
      date_trunc('month', zuora_subscription.subscription_start_date::DATE)     AS sub_start_month,
      date_trunc('month', zuora_subscription.subscription_end_date::DATE)       AS sub_end_month,
      subscription_start_date::DATE                                             AS subscription_start_date,
      subscription_end_date::DATE                                               AS subscription_end_date,
      zuora_rpc.effective_start_month,
      zuora_rpc.effective_end_month,
      zuora_rpc.effective_start_date::DATE                                      AS effective_start_date,
      zuora_rpc.effective_end_date::DATE                                        AS effective_end_date
    FROM zuora_accts
    INNER JOIN zuora_subscription
      ON zuora_accts.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rp
      ON zuora_rp.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_accts.sold_to_contact_id ,zuora_accts.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN zuora_product
      ON zuora_product.product_id = zuora_rpc.product_id
    WHERE zuora_subscription.subscription_status NOT IN ('Draft','Expired')
      AND zuora_rpc.charge_type = 'Recurring'
      AND mrr != 0

), month_base_mrr AS (

    SELECT
      date_actual                               AS mrr_month,
      account_number,
      crm_id,
      account_name,
      account_id,
      subscription_id,
      subscription_name_slugify,
      sub_start_month,
      sub_end_month,
      subscription_start_date,
      subscription_end_date,
      effective_start_month,
      effective_end_month,
      effective_start_date,
      effective_end_date,
      country,
      {{product_category('rate_plan_name')}},
      {{ delivery('product_category')}},
      CASE
        WHEN lower(rate_plan_name) like '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                                       AS service_type,
      product_rate_plan_charge_id,
      rate_plan_name,
      rate_plan_charge_name,
      charge_type,
      unit_of_measure,
      SUM(mrr)                                  AS mrr,
      SUM(quantity)                             AS quantity
    FROM base_mrr
    INNER JOIN date_table
      ON base_mrr.effective_start_month <= date_table.date_actual
      AND (base_mrr.effective_end_month > date_table.date_actual OR base_mrr.effective_end_month IS NULL)
    {{ dbt_utils.group_by(n=24) }}

), current_mrr AS (

    SELECT
      zuora_accts.account_id,
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,
      SUM(zuora_rpc.mrr)    AS total_current_mrr
    FROM zuora_accts
    INNER JOIN zuora_subscription
      ON zuora_accts.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rp
      ON zuora_rp.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    WHERE zuora_subscription.subscription_status NOT IN ('Draft','Expired')
      AND effective_start_date <= current_date
      AND (effective_end_date > current_date OR effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=3) }}

)

SELECT
  mrr_month,
  month_base_mrr.account_id,
  account_number,
  account_name,
  crm_id,
  month_base_mrr.subscription_id,
  month_base_mrr.subscription_name_slugify,
  sub_start_month,
  sub_end_month,
  effective_start_month,
  effective_end_month,
  country,
  product_category,
  delivery,
  service_type,
  product_rate_plan_charge_id,
  rate_plan_name,
  rate_plan_charge_name,
  charge_type,
  unit_of_measure,
  SUM(mrr)                                                              AS mrr,
  SUM(mrr*12)                                                           AS arr,
  SUM(quantity)                                                         AS quantity,
  MAX(total_current_mrr)                                                AS total_current_mrr
FROM month_base_mrr
LEFT JOIN current_mrr
  ON month_base_mrr.subscription_id = current_mrr.subscription_id
{{ dbt_utils.group_by(n=20) }}
