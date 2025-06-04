{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_invoice', 'dim_invoice'),
    ('fct_invoice', 'fct_invoice')
]) }},

ar_basis AS (

/* Adding payment terms and days overdue to invoices */

  SELECT
    DATE(DATE_TRUNC('month', dim_invoice.invoice_date))           AS period,
    fct_invoice.dim_invoice_id,
    DATEDIFF(DAY, dim_invoice.due_date, CURRENT_DATE())           AS days_overdue,
    DATEDIFF(DAY, dim_invoice.invoice_date, dim_invoice.due_date) AS payment_terms,
    fct_invoice.balance                                           AS balance,
    fct_invoice.amount                                            AS invoice_amount
  FROM dim_invoice
  LEFT JOIN fct_invoice 
    ON dim_invoice.dim_invoice_id = fct_invoice.dim_invoice_id
  WHERE dim_invoice.status = 'Posted'
    AND fct_invoice.balance > 0

),

final AS (

/* Adding aging bucket to overdue invoices */

  SELECT
    --Primary key
    dim_invoice_id,

    --Dates
    period,

    --Amounts
    balance,
    invoice_amount,

    --Additive fields
    payment_terms,
    days_overdue,
    CASE
      WHEN days_overdue <= 0
        THEN '1 -- Current'
      WHEN (days_overdue >= 1 AND days_overdue <= 30)
        THEN '2 -- 1 to 30 days past due'
      WHEN (days_overdue >= 31 AND days_overdue <= 60)
        THEN '3 -- 31 to 60 days past due'
      WHEN (days_overdue >= 61 AND days_overdue <= 90)
        THEN '4 -- 61 to 90 days past due'
      WHEN (days_overdue >= 91 AND days_overdue <= 120)
        THEN '5 -- 91 to 120 days past due'
      WHEN days_overdue > 120
        THEN '6 -- More than 120 days past due'
      ELSE 'n/a'
    END AS aging_bucket
  FROM ar_basis
  ORDER BY period, aging_bucket

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-05-08"
) }}
