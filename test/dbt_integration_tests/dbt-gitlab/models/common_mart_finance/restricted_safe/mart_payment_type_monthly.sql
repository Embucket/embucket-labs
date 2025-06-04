{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH payment_by_type AS (

/* Table providing payment amounts and volumes by type, electronic or external, per month */

  SELECT
    DATE(DATE_TRUNC('month', fct_payment.payment_date)) AS period,
    fct_payment.payment_type                            AS payment_type,
    SUM(fct_payment.payment_amount)                     AS payment_amount_by_type,
    COUNT(fct_payment.payment_amount)                   AS number_of_payments_by_type
  FROM {{ ref('fct_payment') }}
  {{ dbt_utils.group_by(n=2)}}
  ORDER BY period, payment_type

),

all_payments AS (

/* Table providing total payment amounts and volumes per month */

  SELECT
    DATE(DATE_TRUNC('month', fct_payment.payment_date)) AS period,
    SUM(fct_payment.payment_amount)                     AS total_payment_amount,
    COUNT(fct_payment.payment_amount)                   AS total_number_of_payments
  FROM {{ ref('fct_payment') }}
  GROUP BY period
  ORDER BY period

),

final AS (

/* Table comparing total payment amounts and volumes per type per month */

  SELECT
    --Primary key
    payment_by_type.period,
    
    --Dates
    dim_date.fiscal_year                                                                                   AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                        AS fiscal_quarter,
    
    --Additive fields
    payment_by_type.payment_type                                                                           AS payment_type,
    
    --Agreggates
    payment_by_type.payment_amount_by_type                                                                 AS payment_amount_by_type,
    all_payments.total_payment_amount                                                                      AS total_payment_amount,
    ROUND(((payment_by_type.payment_amount_by_type / all_payments.total_payment_amount) * 100), 2)         AS percentage_payment_by_type_amount,
    payment_by_type.number_of_payments_by_type                                                             AS number_of_payments_by_type,
    all_payments.total_number_of_payments                                                                  AS total_number_of_payments,
    ROUND(((payment_by_type.number_of_payments_by_type / all_payments.total_number_of_payments) * 100), 2) AS percentage_payment_by_type_count
  
  FROM payment_by_type
  LEFT JOIN all_payments 
    ON payment_by_type.period = all_payments.period
  LEFT JOIN {{ ref('dim_date') }} 
    ON payment_by_type.period = dim_date.date_actual
  ORDER BY payment_by_type.period, payment_by_type.payment_type

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-07",
updated_date="2024-05-07"
) }}
