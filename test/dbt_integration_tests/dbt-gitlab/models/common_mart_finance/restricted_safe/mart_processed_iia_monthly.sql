{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH iia_credit AS (

  /* invoice item adjustment credit */

  SELECT
    DATE(DATE_TRUNC('month', invoice_item_adjustment_date)) AS iia_credit_date,
    SUM(invoice_item_adjustment_amount) * -1                AS amount_credit
  FROM {{ ref('fct_invoice_item_adjustment') }}
  WHERE invoice_item_adjustment_status = 'Processed'
    AND invoice_item_adjustment_type = 'Credit'
  GROUP BY iia_credit_date
  ORDER BY iia_credit_date

),

iia_charge AS (

  /* invoice item adjustment charge */

  SELECT
    DATE(DATE_TRUNC('month', invoice_item_adjustment_date)) AS iia_charge_date,
    SUM(invoice_item_adjustment_amount)                     AS amount_charge
  FROM {{ ref('fct_invoice_item_adjustment') }}
  WHERE invoice_item_adjustment_status = 'Processed'
    AND invoice_item_adjustment_type = 'Charge'
  GROUP BY iia_charge_date
  ORDER BY iia_charge_date

),

final AS (

/* invoice item adjustment credit and charge monthly */

  SELECT
    --Primary key
    COALESCE(iia_credit.iia_credit_date, iia_charge.iia_charge_date) AS period,

    --Aggregated amounts
    COALESCE(iia_credit.amount_credit, 0)                            AS iia_credit_amount,
    COALESCE(iia_charge.amount_charge, 0)                            AS iia_charge_amount
    
  FROM iia_credit
  FULL OUTER JOIN iia_charge ON iia_credit.iia_credit_date = iia_charge.iia_charge_date
  ORDER BY period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-06",
updated_date="2024-05-06"
) }}
