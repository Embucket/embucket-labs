{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('mart_booking_billing_ar_monthly', 'mart_booking_billing_ar_monthly'),
    ('mart_collections_monthly', 'mart_collections_monthly')

]) }},

booking_billing AS (

/* adding booking and billing aggregated amounts per month */

  SELECT
    period,
    ROUND(booking_amount, 2)                                AS booking_amount,
    fiscal_year,
    fiscal_quarter,
    ROUND(invoice_amount_without_tax, 2)                    AS invoice_amount_without_tax,
    ROUND(invoice_tax_amount, 2)                            AS invoice_tax_amount,
    ROUND(invoice_amount_with_tax, 2)                       AS invoice_amount_with_tax,
    ROUND((booking_amount - invoice_amount_without_tax), 2) AS variance_booking_billing
  FROM mart_booking_billing_ar_monthly
  ORDER BY period

),

collections AS (

/* adding payments collected for given invoice month */

  SELECT
    billed_period,
    SUM(payment_applied_to_invoice) AS total_current_collected
  FROM mart_collections_monthly
  GROUP BY billed_period
  ORDER BY billed_period

),

final AS (

/* adding booking, billing and AR data by month */

  SELECT

    --Primary key
    booking_billing.period,

    --Dates
    booking_billing.fiscal_year,
    booking_billing.fiscal_quarter,

    --Agreggates
    booking_billing.booking_amount,
    booking_billing.invoice_amount_without_tax,
    booking_billing.invoice_tax_amount,
    booking_billing.invoice_amount_with_tax,
    booking_billing.variance_booking_billing,
    collections.total_current_collected,
    ROUND(((collections.total_current_collected / booking_billing.invoice_amount_with_tax) * 100), 2) AS percentage_collected_in_period,
    ROUND((100 - percentage_collected_in_period), 2)                                                  AS percentage_out_of_period
  FROM booking_billing
  LEFT JOIN collections
    ON booking_billing.period = collections.billed_period

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-07-04",
updated_date="2024-07-04"
) }}

