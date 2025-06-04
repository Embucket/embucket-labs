{{ config(
    tags=["mnpi_exception"]
) }}

WITH recruiting_expenses AS (

  SELECT
    accounting_period,
    SUM(IFF(department_name = 'Recruiting', actual_amount, 0))            AS recruiting_department,
    SUM(IFF(account_number = '6055', actual_amount, 0))                   AS recruiting_fees,
    SUM(IFF(
      department_name = 'Recruiting'
      AND account_number != '6055'
      AND account_number != '6075', actual_amount, 0
    ))                                                                    AS recruiting_department_minus_overlap,
    SUM(IFF(
      LOWER(transaction_lines_memo) = 'referral bonus'
      OR account_number = '6075', actual_amount, 0
    ))                                                                    AS referral_fees,
    recruiting_department_minus_overlap + recruiting_fees + referral_fees AS total_expenses
  FROM {{ ref ('netsuite_actuals_income_cogs_opex') }}
  GROUP BY 1

),

hires AS (

  SELECT
    DATE_TRUNC(MONTH, hire_date) AS hire_month,
    COUNT(DISTINCT employee_id)  AS hires
  FROM {{ ref ('employee_directory_analysis') }}
  WHERE is_hire_date = TRUE
  GROUP BY 1

),

joined AS (

  SELECT
    hires.hire_month,
    hires.hires,
    recruiting_expenses.recruiting_department,
    recruiting_expenses.recruiting_fees,
    recruiting_expenses.recruiting_department_minus_overlap,
    recruiting_expenses.referral_fees,
    recruiting_expenses.total_expenses,
    recruiting_expenses.total_expenses / hires.hires       AS cost_per_hire,
    SUM(recruiting_expenses.total_expenses) OVER (
      ORDER BY hires.hire_month
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                      AS rolling_3_month_total_expenses,
    SUM(hires.hires) OVER (
      ORDER BY hires.hire_month
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                      AS rolling_3_month_hires,
    rolling_3_month_total_expenses / rolling_3_month_hires AS rolling_3_month_cost_per_hire
  FROM hires
  INNER JOIN recruiting_expenses
    ON hires.hire_month = recruiting_expenses.accounting_period

)

SELECT *
FROM joined
WHERE hire_month > '2019-01-01'
