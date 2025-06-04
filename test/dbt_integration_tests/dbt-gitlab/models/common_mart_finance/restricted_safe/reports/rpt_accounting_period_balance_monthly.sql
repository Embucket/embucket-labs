{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('mart_booking_billing_ar_monthly', 'mart_booking_billing_ar_monthly'),
    ('mart_processed_cba_monthly', 'mart_processed_cba_monthly'),
    ('mart_processed_refunds_monthly', 'mart_processed_refunds_monthly'),
    ('mart_processed_iia_monthly', 'mart_processed_iia_monthly'),
    ('mart_historical_balance_by_payment_terms_bucket_monthly', 'mart_historical_balance_by_payment_terms_bucket_monthly'),
    ('mart_payments_refunds_against_future_invoices', 'mart_payments_refunds_against_future_invoices'),
    ('dim_date', 'dim_date')

]) }},

invoice_amounts AS (

/* Total billing amount monthly including tax */

  SELECT
    period,
    invoice_amount_with_tax AS total_billing
  FROM mart_booking_billing_ar_monthly
  ORDER BY period

),

payment_amounts AS (

/* Total payment amount monthly */

  SELECT
    period,
    payment_amount * -1 AS payments
  FROM mart_booking_billing_ar_monthly
  ORDER BY period

),

overpayments AS (

/* Total overpayment amount monthly */

  SELECT
    period,
    SUM(overpayment) * -1 AS overpayment
  FROM mart_processed_cba_monthly
  GROUP BY period
  ORDER BY period

),

refund_amounts AS (

/* Total refund amount monthly */

  SELECT
    period,
    refund_amount
  FROM mart_processed_refunds_monthly
  ORDER BY period

),

iia_amounts AS (

/* Total invoice item adjustment amounts */

  SELECT
    period,
    iia_charge_amount + iia_credit_amount AS adjustment_amount
  FROM mart_processed_iia_monthly
  ORDER BY period

),

aging_bucket_1 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_current
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '1 -- Current'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_2 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_1_30
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '2 -- 1 to 30 days past due'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_3 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_31_60
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '3 -- 31 to 60 days past due'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_4 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_61_90
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '4 -- 61 to 90 days past due'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_5 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_91_120
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '5 -- 91 to 120 days past due'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_6 AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS ar_more_than_120
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  WHERE aging_bucket = '6 -- More than 120 days past due'
  GROUP BY aging_bucket, period
  ORDER BY period

),

aging_bucket_total AS (

/* Historical aging bucket amounts */

  SELECT
    period,
    SUM(balance) AS total_ar
  FROM mart_historical_balance_by_payment_terms_bucket_monthly
  GROUP BY period
  ORDER BY period

),

credit_balance AS (

/* Credit balance amount monthly */

  SELECT
    period,
    ROUND(credit_balance_per_month, 2) * -1 AS credit_balance
  FROM mart_processed_cba_monthly
  ORDER BY period

),

future_dated_refunds_payments AS (

/* Future date refunds and payments monthly */

  SELECT
    period,
    payments_refunds_against_future_invoices * -1 AS payments_refunds_against_future_invoices
  FROM mart_payments_refunds_against_future_invoices
  ORDER BY period

),

table_basis AS (

/* Joining all data by period monthly */

  SELECT
    invoice_amounts.period,
    COALESCE(invoice_amounts.total_billing, 0)                                          AS total_billing,
    COALESCE(payment_amounts.payments, 0)                                               AS payments,
    COALESCE(payment_amounts.payments, 0) - COALESCE(overpayments.overpayment, 0)       AS payment_without_overpayment,
    COALESCE(overpayments.overpayment, 0)                                               AS overpayments,
    COALESCE(refund_amounts.refund_amount, 0)                                           AS refund_amounts,
    COALESCE(iia_amounts.adjustment_amount, 0)                                          AS adjustment_amount,
    COALESCE(aging_bucket_1.ar_current, 0)                                              AS ar_current,
    COALESCE(aging_bucket_2.ar_1_30, 0)                                                 AS ar_1_30,
    COALESCE(aging_bucket_3.ar_31_60, 0)                                                AS ar_31_60,
    COALESCE(aging_bucket_4.ar_61_90, 0)                                                AS ar_61_90,
    COALESCE(aging_bucket_5.ar_91_120, 0)                                               AS ar_91_120,
    COALESCE(aging_bucket_6.ar_more_than_120, 0)                                        AS ar_more_than_120,
    COALESCE(aging_bucket_total.total_ar, 0)                                            AS total_ar,
    COALESCE(credit_balance.credit_balance, 0)                                          AS credit_balance,
    COALESCE(future_dated_refunds_payments.payments_refunds_against_future_invoices, 0) AS payments_refunds_against_future_invoices
  FROM invoice_amounts
  FULL OUTER JOIN payment_amounts 
    ON invoice_amounts.period = payment_amounts.period
  FULL OUTER JOIN overpayments 
    ON invoice_amounts.period = overpayments.period
  FULL OUTER JOIN refund_amounts 
    ON invoice_amounts.period = refund_amounts.period
  FULL OUTER JOIN iia_amounts 
    ON invoice_amounts.period = iia_amounts.period
  FULL OUTER JOIN aging_bucket_1 
    ON invoice_amounts.period = aging_bucket_1.period
  FULL OUTER JOIN aging_bucket_2 
    ON invoice_amounts.period = aging_bucket_2.period
  FULL OUTER JOIN aging_bucket_3 
    ON invoice_amounts.period = aging_bucket_3.period
  FULL OUTER JOIN aging_bucket_4 
    ON invoice_amounts.period = aging_bucket_4.period
  FULL OUTER JOIN aging_bucket_5 
    ON invoice_amounts.period = aging_bucket_5.period
  FULL OUTER JOIN aging_bucket_6 
    ON invoice_amounts.period = aging_bucket_6.period
  FULL OUTER JOIN aging_bucket_total 
    ON invoice_amounts.period = aging_bucket_total.period
  FULL OUTER JOIN credit_balance 
    ON invoice_amounts.period = credit_balance.period
  FULL OUTER JOIN future_dated_refunds_payments 
    ON invoice_amounts.period = future_dated_refunds_payments.period
  WHERE total_billing > 0
  ORDER BY invoice_amounts.period

),

ending_ar AS (

/* Calculating the ending account receivable */

  SELECT
    *,
    SUM(table_basis.total_billing + table_basis.refund_amounts + table_basis.adjustment_amount + table_basis.payments) OVER (ORDER BY table_basis.period) AS ending_accounts_receivable --running total of the relevant transactions per month
  FROM table_basis

),

final AS (

  SELECT
    --Primary key
    ending_ar.period,

    --Dates
    dim_date.fiscal_year,
    dim_date.fiscal_quarter_name,

    --Aggregates
    COALESCE(LAG(ending_ar.ending_accounts_receivable) OVER (ORDER BY ending_ar.period), 0) AS starting_accounts_receivable, --starting AR equals to the ending AR from the previous month
    ending_ar.total_billing,
    ending_ar.payment_without_overpayment,
    ending_ar.overpayments,
    ending_ar.refund_amounts,
    ending_ar.adjustment_amount,
    ending_ar.ending_accounts_receivable,
    ending_ar.ar_current,
    ending_ar.ar_1_30,
    ending_ar.ar_31_60,
    ending_ar.ar_61_90,
    ending_ar.ar_91_120,
    ending_ar.ar_more_than_120,
    ending_ar.total_ar                                                                      AS total_invoice_aging_balance,
    COALESCE(LAG(total_invoice_aging_balance) OVER (ORDER BY ending_ar.period), 0)          AS starting_total_invoice_aging_balance, --starting AR equals to the ending AR from the previous month
    ending_ar.credit_balance,
    ending_ar.payments_refunds_against_future_invoices
  FROM ending_ar
  LEFT JOIN dim_date 
    ON ending_ar.period = dim_date.date_actual

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-07-04",
updated_date="2024-07-09"
) }}

