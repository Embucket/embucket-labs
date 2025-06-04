{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH increase_cba AS (

  /* credit balance adjustment increase */

  SELECT
    DATE(DATE_TRUNC('month', fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS cba_increase_date,
    SUM(fct_credit_balance_adjustment.credit_balance_adjustment_amount)                     AS increase
  FROM {{ ref('fct_credit_balance_adjustment') }}
  WHERE fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Increase'
    AND fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
  GROUP BY cba_increase_date
  ORDER BY cba_increase_date

),

decrease_cba AS (

  /* credit balance adjustment decrease */

  SELECT
    DATE(DATE_TRUNC('month', fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS cba_decrease_date,
    SUM(fct_credit_balance_adjustment.credit_balance_adjustment_amount)                     AS decrease
  FROM {{ ref('fct_credit_balance_adjustment') }}
  WHERE fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Decrease'
    AND fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
  GROUP BY cba_decrease_date
  ORDER BY cba_decrease_date

),

overpayment AS (

  /* credit balance adjustment increase with no invoice associated indicating an overpayment */

  SELECT
    DATE(DATE_TRUNC('month', fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS overpayment_date,
    SUM(fct_credit_balance_adjustment.credit_balance_adjustment_amount)                     AS overpayment
  FROM {{ ref('fct_credit_balance_adjustment') }}
  WHERE fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Increase'
    AND fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
    AND fct_credit_balance_adjustment.dim_invoice_id = ''
  GROUP BY overpayment_date
  ORDER BY overpayment_date

),

final AS (

  /* Final table showing the cba increase, decrease, the overpayment, the running totals of the increase and decrease and final credit balance in the accounting period */

  SELECT
    --Primary key
    COALESCE(increase_cba.cba_increase_date, decrease_cba.cba_decrease_date) AS period,

    --Aggregated amounts
    COALESCE(increase_cba.increase, 0)                                       AS increase,
    COALESCE(decrease_cba.decrease, 0)                                       AS decrease,
    COALESCE(overpayment.overpayment, 0)                                     AS overpayment,
    SUM(increase_cba.increase) OVER (ORDER BY period)                        AS running_total_increase,
    SUM(decrease_cba.decrease) OVER (ORDER BY period)                        AS running_total_decrease,
    ROUND((running_total_increase - running_total_decrease), 2)              AS credit_balance_per_month
    
  FROM increase_cba
  FULL OUTER JOIN decrease_cba ON increase_cba.cba_increase_date = decrease_cba.cba_decrease_date
  FULL OUTER JOIN overpayment ON increase_cba.cba_increase_date = overpayment.overpayment_date
  ORDER BY period

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-07",
updated_date="2024-05-07"
) }}
