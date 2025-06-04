{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('driveload_invoice_aging_detail_source', 'driveload_invoice_aging_detail_source'),
    ('fct_invoice', 'fct_invoice'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_date', 'dim_date')
]) }},

reseller AS (

/* Assign reseller as channel */

  SELECT
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date)) AS period,
    driveload_invoice_aging_detail_source.account_balance_impact                                AS balance,
    CASE
      WHEN dim_billing_account.ssp_channel = 'Reseller'
        THEN 'Reseller'
      ELSE 'n/a'
    END                                                                                         AS channel
  FROM driveload_invoice_aging_detail_source
  LEFT JOIN fct_invoice 
    ON driveload_invoice_aging_detail_source.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN dim_billing_account 
    ON fct_invoice.dim_billing_account_id = dim_billing_account.dim_billing_account_id
  WHERE dim_billing_account.ssp_channel = 'Reseller'
    AND (dim_billing_account.dim_billing_account_id != '2c92a00c6ccd018d016d02a684e036fa' AND dim_billing_account.dim_billing_account_id != '2c92a00872989ae10172a044c43758f6')

),

non_reseller AS (

/* Assign non-reseller as channel */

  SELECT
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date)) AS period,
    driveload_invoice_aging_detail_source.account_balance_impact                                AS balance,
    CASE
      WHEN dim_billing_account.ssp_channel = 'Non-Reseller'
        THEN 'Non-Reseller'
      ELSE 'n/a'
    END                                                                                         AS channel
  FROM driveload_invoice_aging_detail_source
  LEFT JOIN fct_invoice 
    ON driveload_invoice_aging_detail_source.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN dim_billing_account 
    ON fct_invoice.dim_billing_account_id = dim_billing_account.dim_billing_account_id
  WHERE dim_billing_account.ssp_channel = 'Non-Reseller'

),

alliance AS (

/* Assign alliance as channel */

  SELECT
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date)) AS period,
    driveload_invoice_aging_detail_source.account_balance_impact                                AS balance,
    CASE
      WHEN dim_billing_account.ssp_channel = 'Reseller'
        THEN 'Alliance'
      ELSE 'n/a'
    END                                                                                         AS channel
  FROM driveload_invoice_aging_detail_source
  LEFT JOIN fct_invoice 
    ON driveload_invoice_aging_detail_source.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN dim_billing_account 
    ON fct_invoice.dim_billing_account_id = dim_billing_account.dim_billing_account_id
  WHERE dim_billing_account.dim_billing_account_id = '2c92a00c6ccd018d016d02a684e036fa' OR dim_billing_account.dim_billing_account_id = '2c92a00872989ae10172a044c43758f6'

),

all_channels AS (

/* Union all channels */

  SELECT *
  FROM reseller
  UNION ALL
  SELECT *
  FROM non_reseller
  UNION ALL
  SELECT *
  FROM alliance

),

balance_per_ssp_channel AS (

/* Determine the total balances per channel monthly */

  SELECT
    all_channels.period,
    all_channels.channel,
    SUM(all_channels.balance)   AS total_balance_per_channel,
    COUNT(all_channels.balance) AS invoice_count_per_channel
  FROM all_channels
  {{ dbt_utils.group_by(n=2) }}
  ORDER BY all_channels.period, all_channels.channel

),

total AS (

/* Determine the total balances for all open invoices monthly */

  SELECT
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date)) AS period,
    SUM(account_balance_impact)                                                                 AS total_all_balance,
    COUNT(account_balance_impact)                                                               AS count_all_open_invoices
  FROM driveload_invoice_aging_detail_source
  GROUP BY accounting_period_end_date

),

final AS (

/* Compare balances and count per SSP channel vs. the total balances for all open invoices monthly */

  SELECT
    --Primary key
    balance_per_ssp_channel.period,
    
    --Dates
    dim_date.fiscal_year                                                                                AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                     AS fiscal_quarter,

    --Additive fields
    balance_per_ssp_channel.channel                                                                     AS channel,

    --Aggregates
    balance_per_ssp_channel.total_balance_per_channel                                                   AS total_balance_per_channel,
    ROUND((balance_per_ssp_channel.total_balance_per_channel / total.total_all_balance) * 100, 2)       AS percentage_of_open_balance_per_path,
    total.total_all_balance                                                                             AS total_all_balance,
    balance_per_ssp_channel.invoice_count_per_channel                                                   AS invoice_count_per_channel,
    ROUND((balance_per_ssp_channel.invoice_count_per_channel / total.count_all_open_invoices) * 100, 2) AS percentage_of_open_invoices_count_per_path,
    total.count_all_open_invoices                                                                       AS count_all_open_invoices
  
  FROM balance_per_ssp_channel
  LEFT JOIN total 
    ON balance_per_ssp_channel.period = total.period
  LEFT JOIN dim_date 
    ON total.period = dim_date.date_actual
  ORDER BY balance_per_ssp_channel.period, balance_per_ssp_channel.channel

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-07",
updated_date="2024-07-03"
) }}
