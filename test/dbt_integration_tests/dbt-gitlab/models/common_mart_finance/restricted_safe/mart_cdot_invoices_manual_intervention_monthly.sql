{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_invoice', 'dim_invoice'),
    ('fct_invoice_item_adjustment', 'fct_invoice_item_adjustment'),
    ('fct_credit_balance_adjustment', 'fct_credit_balance_adjustment'),
    ('fct_refund_invoice_payment', 'fct_refund_invoice_payment'),
    ('dim_date', 'dim_date')
]) }},

cdot_created_invoices AS (

/* Determine all invoices that were posted via CDot or API automation e.g. auto-renewal, QSR */

  SELECT
    DATE(DATE_TRUNC('month', invoice_date)) AS period,
    dim_invoice_id
  FROM dim_invoice
  WHERE status = 'Posted'
    AND (
      created_by_id = '2c92a0fd55822b4d015593ac264767f2'
      OR created_by_id = '2c92a0107bde3653017bf00cd8a86d5a'
    )

),

manually_modified_invoices AS (

/* Determine all invoices that were posted via CDot or API automation e.g. auto-renewal, QSR which were manually updated */

  SELECT DISTINCT
    DATE(DATE_TRUNC('month', invoice_date)) AS period,
    dim_invoice.dim_invoice_id
  FROM dim_invoice
  LEFT JOIN fct_invoice_item_adjustment 
    ON dim_invoice.dim_invoice_id = fct_invoice_item_adjustment.dim_invoice_id
  LEFT JOIN fct_credit_balance_adjustment 
    ON dim_invoice.dim_invoice_id = fct_credit_balance_adjustment.dim_invoice_id
  LEFT JOIN fct_refund_invoice_payment 
    ON dim_invoice.dim_invoice_id = fct_refund_invoice_payment.dim_invoice_id
  WHERE dim_invoice.status = 'Posted'
    AND fct_invoice_item_adjustment.dim_invoice_id IS NULL
    AND fct_credit_balance_adjustment.dim_invoice_id IS NULL
    AND fct_refund_invoice_payment.dim_invoice_id IS NULL
    AND (created_by_id = '2c92a0fd55822b4d015593ac264767f2' OR created_by_id = '2c92a0107bde3653017bf00cd8a86d5a')
),

cdot_invoices_manual_intervention_monthly AS (

/* Calculate the aggregates monthly */

  SELECT
    cdot_created_invoices.period,
    COUNT(cdot_created_invoices.dim_invoice_id)                                                    AS count_all_cdot_invoices,
    COUNT(cdot_created_invoices.dim_invoice_id) - COUNT(manually_modified_invoices.dim_invoice_id) AS count_cdot_modified_invoices,
    ROUND(((count_cdot_modified_invoices / count_all_cdot_invoices) * 100), 2)                     AS percentage_manually_modified_cdot_invoices

  FROM cdot_created_invoices
  LEFT JOIN manually_modified_invoices 
    ON cdot_created_invoices.dim_invoice_id = manually_modified_invoices.dim_invoice_id
  GROUP BY cdot_created_invoices.period
  ORDER BY cdot_created_invoices.period

),

final AS (

/* Add fiscal year and quarter */


  SELECT
    --Primary key
    cdot_invoices_manual_intervention_monthly.period,

    --Dates
    dim_date.fiscal_year                                                                 AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                      AS fiscal_quarter,

    --Aggregates
    cdot_invoices_manual_intervention_monthly.count_all_cdot_invoices                    AS count_all_cdot_invoices,
    cdot_invoices_manual_intervention_monthly.count_cdot_modified_invoices               AS count_cdot_modified_invoices,
    cdot_invoices_manual_intervention_monthly.percentage_manually_modified_cdot_invoices AS percentage_manually_modified_cdot_invoices

  FROM cdot_invoices_manual_intervention_monthly
  LEFT JOIN dim_date 
    ON cdot_invoices_manual_intervention_monthly.period = dim_date.date_actual

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-05-08"
) }}
