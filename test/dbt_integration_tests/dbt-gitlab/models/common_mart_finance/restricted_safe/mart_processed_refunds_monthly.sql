{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH final AS (

/* View sum of refund amounts processed monthly; period indicates accounting period in the billing system */

  SELECT
    --Primary key
    DATE_TRUNC('month', (DATE(refund_date))) AS period,

    --Aggregated amounts
    SUM(refund_amount)                       AS refund_amount

  FROM {{ ref('fct_refund') }}
  WHERE refund_status = 'Processed'
  GROUP BY period
  ORDER BY period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-07",
updated_date="2024-05-07"
) }}

