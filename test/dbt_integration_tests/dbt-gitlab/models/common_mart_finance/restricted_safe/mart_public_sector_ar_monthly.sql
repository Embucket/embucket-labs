{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('driveload_invoice_aging_detail_source', 'driveload_invoice_aging_detail_source'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_invoice', 'dim_invoice'),
    ('dim_date', 'dim_date')
]) }},

segment AS (

/* Determine purchase segment of open invoices monthly */

  SELECT DISTINCT
    driveload_invoice_aging_detail_source.dim_invoice_id,
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date))  AS period,
    driveload_invoice_aging_detail_source.account_balance_impact,
    CASE
      WHEN dim_crm_opportunity.user_segment = 'PUBSEC'
        THEN 'PubSec'
      ELSE 'Non-PubSec'
    END                                                                                          AS segment
  FROM driveload_invoice_aging_detail_source
  LEFT JOIN dim_invoice 
    ON driveload_invoice_aging_detail_source.dim_invoice_id = dim_invoice.dim_invoice_id
  LEFT JOIN dim_crm_opportunity 
    ON dim_invoice.invoice_number = dim_crm_opportunity.invoice_number

),

balance_per_segment AS (

/* Determine the total balances per purchase segment monthly */

  SELECT
    segment.period,
    segment.segment,
    SUM(segment.account_balance_impact)   AS total_balance_per_segment,
    COUNT(segment.account_balance_impact) AS invoice_count_per_segment
  FROM segment
  {{ dbt_utils.group_by(n=2)}}
  ORDER BY segment.period, segment.segment

),

total AS (

/* Determine the total balances for all open invoices monthly */

  SELECT
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date))  AS period,
    SUM(account_balance_impact)                                                                  AS total_all_balance,
    COUNT(account_balance_impact)                                                                AS count_all_open_invoices
  FROM driveload_invoice_aging_detail_source
  GROUP BY period

),

final AS (

/* Compare balances and count per segment vs. the total balances for all open invoices monthly */

  SELECT
    --Primary key
    balance_per_segment.period,

    --Dates
    dim_date.fiscal_year                                                                            AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                 AS fiscal_quarter,

    --Additive fields
    balance_per_segment.segment                                                                     AS segment,

    --Aggregates
    ROUND((balance_per_segment.total_balance_per_segment / total.total_all_balance) * 100, 2)       AS percentage_of_open_balance_per_segment,
    balance_per_segment.total_balance_per_segment                                                   AS total_balance_per_segment,
    ROUND((balance_per_segment.invoice_count_per_segment / total.count_all_open_invoices) * 100, 2) AS percentage_of_open_invoices_count_per_segment,
    balance_per_segment.invoice_count_per_segment                                                   AS invoice_count_per_segment

  FROM balance_per_segment
  LEFT JOIN total 
    ON balance_per_segment.period = total.period
  LEFT JOIN dim_date 
    ON balance_per_segment.period = dim_date.date_actual

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-07-03"
) }}
