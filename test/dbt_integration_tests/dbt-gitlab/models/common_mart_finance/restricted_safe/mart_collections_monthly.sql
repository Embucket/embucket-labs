{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('fct_invoice_payment', 'fct_invoice_payment'),
    ('dim_invoice', 'dim_invoice'),
    ('fct_invoice', 'fct_invoice'),
    ('fct_payment', 'fct_payment'),
    ('dim_date', 'dim_date')
]) }},

invoice_detail AS (

/* Selecting the applied payment amount with status ‘processed’, invoice month and payment month */

  SELECT
    DATE(DATE_TRUNC('month', fct_payment.payment_date)) AS payment_period,
    DATE(DATE_TRUNC('month', dim_invoice.invoice_date)) AS invoice_period,
    SUM(fct_invoice_payment.invoice_payment_amount)     AS payment_applied_to_invoice
  FROM fct_invoice_payment
  LEFT JOIN dim_invoice 
    ON fct_invoice_payment.dim_invoice_id = dim_invoice.dim_invoice_id
  LEFT JOIN fct_payment 
    ON fct_invoice_payment.payment_id = fct_payment.payment_id
  WHERE fct_payment.payment_status = 'Processed'
  {{ dbt_utils.group_by(n=2)}}
  ORDER BY payment_period, invoice_period

),

total_billed AS (

/* Total billed per month */

  SELECT
    DATE(DATE_TRUNC('month', dim_invoice.invoice_date)) AS billed_period,
    SUM(fct_invoice.amount)                             AS total_billed
  FROM dim_invoice
    JOIN fct_invoice 
      ON dim_invoice.dim_invoice_id = fct_invoice.dim_invoice_id
  WHERE dim_invoice.status = 'Posted'
  GROUP BY billed_period

),

final AS (

/* Combined the applied payment with billed data per month */

  SELECT
    --Primary key
    invoice_detail.payment_period,
    total_billed.billed_period,

    --Dates
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name                                                              AS fiscal_quarter,
    DATEDIFF(MONTH, total_billed.billed_period, invoice_detail.payment_period)                AS previous_period,

    --Aggregates
    invoice_detail.payment_applied_to_invoice,
    total_billed.total_billed,
    ROUND(((invoice_detail.payment_applied_to_invoice / total_billed.total_billed) * 100), 2) AS percentage_payment_applied_to_billed

  FROM invoice_detail
  INNER JOIN total_billed 
    ON invoice_detail.invoice_period = total_billed.billed_period
  LEFT JOIN dim_date 
    ON invoice_detail.payment_period = dim_date.date_actual

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-08-22"
) }}
