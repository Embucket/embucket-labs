WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

), sfdc_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_deleted_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_deleted_accounts') }}

), zuora_accounts AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_invoices AS (

    SELECT *
    FROM {{ ref('zuora_invoice_charges') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}
    WHERE is_deleted = FALSE

), zuora_product_rp AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), zuora_product_rpc AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_source') }}

), zuora_product_rpct AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_tier_source') }}

), initial_join_to_sfdc AS (

  SELECT
    invoice_number,
    invoice_item_id,
    zuora_accounts.crm_id                                AS invoice_crm_id,
    sfdc_accounts.account_id                             AS sfdc_account_id_int,
    zuora_accounts.account_name,
    invoice_date,
    DATE_TRUNC('month',invoice_date)                     AS invoice_month,
    product_name,
    product_rate_plan_charge_id,
    {{ product_category('rate_plan_name') }},
    rate_plan_name,
    charge_type,
    invoice_item_unit_price,
    quantity                                             AS quantity,
    invoice_item_charge_amount                           AS invoice_item_charge_amount
  FROM zuora_invoices
  LEFT JOIN zuora_accounts
    ON zuora_invoices.invoice_account_id = zuora_accounts.account_id
  LEFT JOIN sfdc_accounts
    ON zuora_accounts.crm_id = sfdc_accounts.account_id
  WHERE invoice_item_charge_amount != 0

), replace_sfdc_account_id_with_master_record_id AS (

    SELECT
      COALESCE(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
      initial_join_to_sfdc.*
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
      ON initial_join_to_sfdc.invoice_crm_id = sfdc_deleted_accounts.sfdc_account_id

), joined AS (

    SELECT
      invoice_number,
      invoice_item_id,
      sfdc_account_id,
      CASE
        WHEN ultimate_parent_account_segment = 'Unknown' THEN 'SMB'
        WHEN ultimate_parent_account_segment = '' THEN 'SMB'
        ELSE ultimate_parent_account_segment
      END                                     AS ultimate_parent_segment,
      replace_account_id.account_name,
      invoice_date,
      invoice_month,
      product_name,
      product_rate_plan_charge_id,
      product_category,
      account_type,
      rate_plan_name,
      charge_type,
      invoice_item_unit_price,
      quantity                                AS quantity,
      invoice_item_charge_amount              AS invoice_item_charge_amount
    FROM replace_sfdc_account_id_with_master_record_id replace_account_id
    LEFT JOIN sfdc_accounts
      ON replace_account_id.sfdc_account_id = sfdc_accounts.account_id

), list_price AS (

  SELECT
    zuora_product_rp.product_rate_plan_name,
    zuora_product_rpc.product_rate_plan_charge_name,
    zuora_product_rpc.product_rate_plan_charge_id,
    MIN(zuora_product_rpct.price)             AS billing_list_price
  FROM zuora_product
  INNER JOIN zuora_product_rp
    ON zuora_product.product_id = zuora_product_rp.product_id
  INNER JOIN zuora_product_rpc
    ON zuora_product_rp.product_rate_plan_id = zuora_product_rpc.product_rate_plan_id
  INNER JOIN zuora_product_rpct
    ON zuora_product_rpc.product_rate_plan_charge_id = zuora_product_rpct.product_rate_plan_charge_id
  WHERE zuora_product.effective_start_date <= CURRENT_DATE
    AND zuora_product_rpct.currency = 'USD'
  GROUP BY 1,2,3
  ORDER BY 1,2

)

SELECT
  joined.invoice_number,
  joined.invoice_item_id,
  sfdc_account_id,
  account_name,
  account_type,
  invoice_date,
  joined.product_name,
  joined.rate_plan_name,
  quantity,
  invoice_item_unit_price,
  invoice_item_charge_amount,
  CASE
    WHEN LOWER(rate_plan_name) LIKE '%month%'   THEN (invoice_item_unit_price * 12)
    WHEN LOWER(rate_plan_name) LIKE '%2 years%' THEN (invoice_item_unit_price/2)
    WHEN LOWER(rate_plan_name) LIKE '%2 year%'  THEN (invoice_item_unit_price/2)
    WHEN LOWER(rate_plan_name) LIKE '%3 years%' THEN (invoice_item_unit_price/3)
    WHEN LOWER(rate_plan_name) LIKE '%3 year%'  THEN (invoice_item_unit_price/3)
    WHEN LOWER(rate_plan_name) LIKE '%4 years%' THEN (invoice_item_unit_price/4)
    WHEN LOWER(rate_plan_name) LIKE '%4 year%'  THEN (invoice_item_unit_price/4)
    WHEN LOWER(rate_plan_name) LIKE '%5 years%' THEN (invoice_item_unit_price/5)
    WHEN LOWER(rate_plan_name) LIKE '%5 year%'  THEN (invoice_item_unit_price/5)
    ELSE invoice_item_unit_price
  END                                           AS annual_price,
  quantity * annual_price                       AS quantity_times_annual,
  ultimate_parent_segment,
  product_category,
  invoice_month,
  fiscal_quarter_name_fy                        AS fiscal_period,
  CASE
    WHEN LOWER(rate_plan_name) LIKE '%month%'   THEN (billing_list_price*12)
    WHEN LOWER(rate_plan_name) LIKE '%2 years%' THEN (billing_list_price/2)
    WHEN LOWER(rate_plan_name) LIKE '%2 year%'  THEN (billing_list_price/2)
    WHEN LOWER(rate_plan_name) LIKE '%3 years%' THEN (billing_list_price/3)
    WHEN LOWER(rate_plan_name) LIKE '%3 year%'  THEN (billing_list_price/3)
    WHEN LOWER(rate_plan_name) LIKE '%4 years%' THEN (billing_list_price/4)
    WHEN LOWER(rate_plan_name) LIKE '%4 year%'  THEN (billing_list_price/4)
    WHEN LOWER(rate_plan_name) LIKE '%5 years%' THEN (billing_list_price/5)
    WHEN LOWER(rate_plan_name) LIKE '%5 year%'  THEN (billing_list_price/5)
    WHEN LOWER(charge_type) != 'recurring' THEN 0
    ELSE billing_list_price
  END                                           AS list_price,
  CASE
    WHEN annual_price = list_price THEN 0
    WHEN LOWER(charge_type) != 'recurring' THEN 0
    ELSE ((annual_price - list_price)/NULLIF(list_price,0)) * -1
  END                                           AS discount,
  CASE
    WHEN LOWER(charge_type) != 'recurring' THEN 0
    ELSE quantity * list_price
  END                                           AS list_price_times_quantity
FROM joined
LEFT JOIN list_price
  ON joined.product_rate_plan_charge_id = list_price.product_rate_plan_charge_id
LEFT JOIN date_table
  ON joined.invoice_month = date_table.date_actual
ORDER BY invoice_date, invoice_number
