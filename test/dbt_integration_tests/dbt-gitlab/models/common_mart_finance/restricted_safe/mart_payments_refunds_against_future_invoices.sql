{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('fct_invoice_payment', 'fct_invoice_payment'),
    ('fct_invoice', 'fct_invoice'),
    ('dim_invoice', 'dim_invoice'),
    ('fct_payment', 'fct_payment'),
    ('fct_refund_invoice_payment', 'fct_refund_invoice_payment'),
    ('fct_refund', 'fct_refund')
]) }},

invoice_payment AS (

/* Payments applied to invoices joining invoice information */

  SELECT
    DATE(DATE_TRUNC('month', dim_invoice.invoice_date)) AS invoice_month,
    fct_invoice_payment.invoice_payment_amount          AS applied_payment_amount,
    fct_invoice_payment.dim_invoice_id,
    fct_invoice_payment.payment_id
  FROM fct_invoice_payment
  LEFT JOIN fct_invoice 
    ON fct_invoice_payment.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN dim_invoice 
    ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id

),

payment AS (

/* Select all payments to determine the payment date */

  SELECT
    DATE(DATE_TRUNC('month', fct_payment.payment_date)) AS payment_month,
    fct_payment.payment_amount                          AS payment_amount,
    fct_payment.payment_id
  FROM fct_payment

),

invoice_payment_and_payment AS (

/* Determine payments that were applied to invoices with future dates */

  SELECT
    payment.payment_month,
    payment.payment_amount
  FROM invoice_payment
  INNER JOIN payment 
    ON invoice_payment.payment_id = payment.payment_id
  WHERE invoice_payment.invoice_month > payment.payment_month

),

invoice_refund AS (

/* Refunds made off the invoices joining invoice information */

  SELECT
    DATE(DATE_TRUNC('month', dim_invoice.invoice_date))      AS invoice_month,
    fct_refund_invoice_payment.refund_invoice_payment_amount AS applied_payment_amount,
    fct_refund_invoice_payment.dim_invoice_id,
    fct_refund_invoice_payment.refund_id
  FROM fct_refund_invoice_payment
  LEFT JOIN fct_invoice 
    ON fct_refund_invoice_payment.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN dim_invoice 
    ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id

),

refund AS (

/* Select all refunds to determine the payment date */

  SELECT
    DATE(DATE_TRUNC('month', fct_refund.refund_date)) AS payment_month,
    fct_refund.refund_amount                          AS payment_amount,
    fct_refund.refund_id
  FROM fct_refund

),

invoice_refund_and_refund AS (

/* Determine refunds that were made off the invoices with future dates */

  SELECT
    refund.payment_month,
    refund.payment_amount
  FROM invoice_refund
  INNER JOIN refund 
    ON invoice_refund.refund_id = refund.refund_id
  WHERE invoice_refund.invoice_month > refund.payment_month
),

payment_refund AS (

  /* Unioning payments and refunds based off future date invoices */

  SELECT
    invoice_payment_and_payment.payment_month       AS period,
    SUM(invoice_payment_and_payment.payment_amount) AS payments_refunds_against_future_invoices
  FROM invoice_payment_and_payment
  GROUP BY period
  UNION ALL
  SELECT
    invoice_refund_and_refund.payment_month            AS period,
    SUM(invoice_refund_and_refund.payment_amount) * -1 AS payments_refunds_against_future_invoices
  FROM invoice_refund_and_refund
  GROUP BY period
  ORDER BY period ASC

),


final AS (

  /* Sum of payemnts and refunds made based off future dated invoices */

  SELECT
    --Primary key
    period,

    --Aggregated amounts
    SUM(payments_refunds_against_future_invoices) AS payments_refunds_against_future_invoices
    
  FROM payment_refund
  GROUP BY period
  ORDER BY period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-06",
updated_date="2024-05-06"
) }}
