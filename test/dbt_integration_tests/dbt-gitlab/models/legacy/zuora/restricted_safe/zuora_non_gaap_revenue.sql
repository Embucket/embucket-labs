WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}

), zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_acct_period AS (

    SELECT *
    FROM {{ ref('zuora_accounting_period_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_rev_sch AS (

    SELECT *
    FROM {{ ref('zuora_revenue_schedule_item_source') }}

), zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), non_gaap_revenue AS (

    SELECT
      zuora_acct_period.accounting_period_start_date::DATE    AS accounting_period,

      -- account info
      zuora_accts.account_name,
      zuora_accts.account_number,
      zuora_accts.crm_id,
      zuora_contact.country,
      zuora_accts.currency,

      --rate_plan info
      zuora_rp.rate_plan_name,
      zuora_rpc.rate_plan_charge_name,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category')}},
      zuora_product.product_name,
      SUM(zuora_rev_sch.revenue_schedule_item_amount)         AS revenue_amt
    FROM zuora_rev_sch
    INNER JOIN zuora_accts
      ON zuora_rev_sch.account_id = zuora_accts.account_id
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_accts.sold_to_contact_id ,zuora_accts.bill_to_contact_id) = zuora_contact.contact_id
    INNER JOIN zuora_rpc
      ON zuora_rev_sch.rate_plan_charge_id = zuora_rpc.rate_plan_charge_id
    INNER JOIN zuora_rp
      ON zuora_rp.rate_plan_id = zuora_rpc.rate_plan_id
    INNER JOIN zuora_acct_period
      ON zuora_acct_period.accounting_period_id = zuora_rev_sch.accounting_period_id
    LEFT JOIN zuora_product
      ON zuora_product.product_id = zuora_rev_sch.product_id
    {{ dbt_utils.group_by(n=11) }}

)

SELECT *
FROM non_gaap_revenue
