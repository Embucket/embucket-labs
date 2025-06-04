WITH map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE LOWER(batch) != 'batch20'
      AND is_deleted = FALSE

), zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_source') }}
    WHERE is_deleted = FALSE

), zuora_invoice_item AS (

    SELECT *
    FROM  {{ ref('zuora_invoice_item_source') }}
    WHERE is_deleted = FALSE

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}
    WHERE is_deleted = FALSE

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}
    WHERE is_deleted = FALSE

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_revenue_bill AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_bill_source') }}

), base_charges AS (

    SELECT
      zuora_account.account_id                                                  AS billing_account_id_subscription,
      map_merged_crm_account.dim_crm_account_id                                 AS crm_account_id_subscription,
      zuora_subscription.subscription_id,
      zuora_rate_plan_charge.rate_plan_charge_id                                AS charge_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan.rate_plan_name,
      zuora_rate_plan_charge.segment                                            AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                                            AS rate_plan_charge_version,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.mrr*12                                             AS arr,
      zuora_rate_plan_charge.quantity,
      DATE_TRUNC('month',zuora_rate_plan_charge.effective_start_date::DATE)     AS effective_start_month,
      DATE_TRUNC('month',zuora_rate_plan_charge.effective_end_date::DATE)       AS effective_end_month
    FROM zuora_account
    INNER JOIN zuora_subscription
      ON zuora_account.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rate_plan
      ON zuora_subscription.subscription_id = zuora_rate_plan.subscription_id
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id

), invoice_charges AS (

    SELECT
      zuora_invoice_item.invoice_item_id                AS invoice_item_id,
      zuora_invoice.invoice_id                          AS invoice_id,
      zuora_invoice.invoice_number,
      zuora_invoice.invoice_date::DATE                  AS invoice_date,
      zuora_invoice_item.service_start_date::DATE       AS service_start_date,
      zuora_invoice_item.service_end_date::DATE         AS service_end_date,
      zuora_invoice.account_id                          AS billing_account_id_invoice,
      map_merged_crm_account.dim_crm_account_id         AS crm_account_id_invoice,
      zuora_invoice_item.rate_plan_charge_id            AS charge_id,
      zuora_invoice_item.charge_name                    AS charge_name,
      zuora_invoice_item.product_rate_plan_charge_id    AS product_details_id,
      zuora_invoice_item.sku                            AS sku,
      zuora_invoice_item.tax_amount                     AS tax_amount_sum,
      zuora_invoice.amount_without_tax                  AS invoice_amount_without_tax,
      zuora_invoice_item.charge_amount                  AS invoice_item_charge_amount,
      zuora_invoice_item.unit_price                     AS invoice_item_unit_price
    FROM zuora_invoice_item
    INNER JOIN zuora_invoice
      ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    INNER JOIN zuora_account
      ON zuora_invoice.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    WHERE zuora_invoice.status='Posted'

), final AS (

    SELECT
      invoice_charges.invoice_item_id,
      invoice_charges.invoice_id                    AS dim_invoice_id,
      base_charges.billing_account_id_subscription  AS dim_billing_account_id_subscription,
      base_charges.crm_account_id_subscription      AS dim_crm_account_id_subscription,
      invoice_charges.billing_account_id_invoice    AS dim_billing_account_id_invoice,
      invoice_charges.crm_account_id_invoice        AS dim_crm_account_id_invoice,
      base_charges.subscription_id                  AS dim_subscription_id,
      zuora_revenue_bill.revenue_contract_line_id   AS dim_revenue_contract_line_id,
      invoice_charges.charge_id,
      invoice_charges.product_details_id            AS dim_product_detail_id,
      invoice_charges.invoice_number,
      invoice_charges.invoice_date,
      invoice_charges.service_start_date,
      invoice_charges.service_end_date,
      base_charges.effective_start_month,
      base_charges.effective_end_month,
      base_charges.quantity,
      base_charges.mrr,
      base_charges.arr,
      invoice_charges.invoice_item_charge_amount,
      CASE
        WHEN LOWER(rate_plan_name)                  LIKE '%month%'
          OR LOWER(rate_plan_charge_name)           LIKE '%month%'
          OR LOWER(charge_name)                     LIKE '%month%'
          THEN (invoice_item_unit_price*12)
        WHEN LOWER(rate_plan_name)                  LIKE '%2 year%'
          OR LOWER(rate_plan_charge_name)           LIKE '%2 year%'
          OR LOWER(charge_name)                     LIKE '%2 year%'
          THEN (invoice_item_unit_price/2)
        WHEN LOWER(rate_plan_name)                  LIKE '%3 year%'
          OR LOWER(rate_plan_charge_name)           LIKE '%3 year%'
          OR LOWER(charge_name)                     LIKE '%3 year%'
          THEN (invoice_item_unit_price/3)
        WHEN LOWER(rate_plan_name)                  LIKE '%4 year%'
          OR LOWER(rate_plan_charge_name)           LIKE '%4 year%'
          OR LOWER(charge_name)                     LIKE '%4 year%'
          THEN (invoice_item_unit_price/4)
        WHEN LOWER(rate_plan_name)                  LIKE '%5 year%'
          OR LOWER(rate_plan_charge_name)           LIKE '%5 year%'
          OR LOWER(charge_name)                     LIKE '%5 year%'
          THEN (invoice_item_unit_price/5)
        ELSE invoice_item_unit_price
      END                                           AS annual_billing_invoice_price,
      invoice_charges.invoice_item_unit_price,
      invoice_charges.invoice_amount_without_tax,
      invoice_charges.tax_amount_sum,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number, rate_plan_charge_segment
          ORDER BY rate_plan_charge_version DESC, service_start_date DESC) = 1,
          TRUE, FALSE
      )                                 AS is_last_segment_version
    FROM base_charges
    INNER JOIN invoice_charges
      ON base_charges.charge_id = invoice_charges.charge_id
    LEFT JOIN zuora_revenue_bill
      ON invoice_charges.invoice_item_id = zuora_revenue_bill.invoice_item_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@chrissharp",
    created_date="2021-01-15",
    updated_date="2023-10-26"
) }}
