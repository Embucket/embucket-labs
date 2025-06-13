{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('driveload_invoice_aging_detail_source', 'driveload_invoice_aging_detail_source'),
    ('dim_invoice', 'dim_invoice'),
    ('dim_date', 'dim_date')
]) }},

final AS (

/* View of historical balances with payment terms and aging bucket by month */

  SELECT
    --Primary key
    DATE_TRUNC('month', DATE(driveload_invoice_aging_detail_source.accounting_period_end_date)) AS period,

    --Dates
    dim_date.fiscal_year                                                                        AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                             AS fiscal_quarter,

    --Aggregated amounts
    driveload_invoice_aging_detail_source.account_balance_impact                                AS balance,

    --Additive fields
    DATEDIFF(DAY, dim_invoice.invoice_date, dim_invoice.due_date)                               AS payment_terms,
    CASE
      WHEN days_overdue <= 0
        THEN '1 -- Current'
      WHEN days_overdue >= 1 AND days_overdue <= 30
        THEN '2 -- 1 to 30 days past due'
      WHEN days_overdue >= 31 AND days_overdue <= 60
        THEN '3 -- 31 to 60 days past due'
      WHEN days_overdue >= 61 AND days_overdue <= 90
        THEN '4 -- 61 to 90 days past due'
      WHEN days_overdue >= 91 AND days_overdue <= 120
        THEN '5 -- 91 to 120 days past due'
      WHEN days_overdue > 120
        THEN '6 -- More than 120 days past due'
      ELSE 'n/a'
    END                                                                                         AS aging_bucket
    
  FROM driveload_invoice_aging_detail_source
  LEFT JOIN dim_invoice 
    ON driveload_invoice_aging_detail_source.dim_invoice_id = dim_invoice.dim_invoice_id
  LEFT JOIN dim_date 
    ON dim_date.date_actual = period

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-09",
updated_date="2024-07-03"
) }}
