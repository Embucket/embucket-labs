WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

), zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice') }}

), zuora_invoice_item AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}
    WHERE is_deleted = FALSE

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription') }}

), base_charges AS (

    SELECT
      zuora_account.account_id,
      zuora_account.crm_id,
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.subscription_status,
      zuora_subscription.version                          AS subscription_version,
      zuora_rate_plan.rate_plan_name,
      zuora_rate_plan_charge.rate_plan_charge_id,
      zuora_rate_plan_charge.product_rate_plan_charge_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.segment                      AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                      AS rate_plan_charge_version,
      zuora_rate_plan_charge.effective_start_date::DATE   AS effective_start_date,
      zuora_rate_plan_charge.effective_end_date::DATE     AS effective_end_date,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.delta_tcv,
      zuora_rate_plan_charge.charge_type,
      zuora_product.product_name
    FROM zuora_account
    INNER JOIN zuora_subscription
      ON zuora_account.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rate_plan
      ON zuora_subscription.subscription_id = zuora_rate_plan.subscription_id
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    LEFT JOIN zuora_product
      ON zuora_rate_plan_charge.product_id = zuora_product.product_id

), invoice_charges AS (

    SELECT
      zuora_invoice.invoice_number,
      zuora_invoice_item.invoice_item_id,
      zuora_invoice.account_id                      AS invoice_account_id,
      zuora_invoice.invoice_date::DATE              AS invoice_date,
      zuora_invoice_item.service_start_date::DATE   AS service_start_date,
      zuora_invoice_item.service_end_date::DATE     AS service_end_date,
      zuora_invoice.amount_without_tax              AS invoice_amount_without_tax,
      zuora_invoice_item.charge_amount              AS invoice_item_charge_amount,
      zuora_invoice_item.unit_price                 AS invoice_item_unit_price,
      zuora_invoice_item.rate_plan_charge_id
    FROM zuora_invoice_item
    INNER JOIN zuora_invoice
      ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    WHERE zuora_invoice.status = 'Posted'

), final AS (

    SELECT
      base_charges.*,
      ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number
          ORDER BY rate_plan_charge_segment, rate_plan_charge_version, service_start_date
      ) AS segment_version_order,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number, rate_plan_charge_segment
          ORDER BY rate_plan_charge_version DESC, service_start_date DESC) = 1,
          TRUE, FALSE
      ) AS is_last_segment_version,
      invoice_account_id,
      invoice_number,
      invoice_item_id,
      invoice_date,
      service_start_date,
      service_end_date,
      invoice_amount_without_tax,
      invoice_item_charge_amount,
      invoice_item_unit_price
    FROM base_charges
    INNER JOIN invoice_charges
      ON base_charges.rate_plan_charge_id = invoice_charges.rate_plan_charge_id

)

SELECT *
FROM final
