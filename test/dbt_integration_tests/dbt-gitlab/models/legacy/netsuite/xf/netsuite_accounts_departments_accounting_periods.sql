WITH accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

), cost_category AS (

     SELECT *
     FROM {{ ref('netsuite_expense_cost_category') }}

), date_details AS (

     SELECT DISTINCT
       first_day_of_month,
       fiscal_year,
       fiscal_quarter,
       fiscal_quarter_name
     FROM {{ ref('date_details') }}

), departments AS (

     SELECT *
     FROM {{ ref('netsuite_departments_xf') }}

), accts_depts AS (

     SELECT DISTINCT
       accts.account_number || ' - ' || accts.account_name  AS unique_account_name,
       accts.account_number,
       accts.account_name,
       depts.parent_department_name,
       depts.department_name
     FROM accounts accts
     CROSS JOIN departments depts
     WHERE accts.is_account_inactive = false
     ORDER BY 2,3,4

), accts_depts_periods AS (

     SELECT DISTINCT
       accts_depts.unique_account_name,
       accts_depts.account_number,
       accts_depts.account_name,
       accts_depts.parent_department_name,
       accts_depts.department_name,
       dd.first_day_of_month                                 AS accounting_period,
       dd.fiscal_year
     FROM accts_depts
     CROSS JOIN date_details dd
     WHERE DATE_TRUNC('year',first_day_of_month)
       BETWEEN DATEADD('year',-2,DATE_TRUNC('year',CURRENT_DATE))
         AND DATEADD('year',2,DATE_TRUNC('year',CURRENT_DATE))

), accts_depts_periods_cost AS (

     SELECT DISTINCT
       adp.fiscal_year,
       adp.unique_account_name,
       adp.account_number,
       adp.parent_department_name,
       adp.department_name,
       cc.cost_category_level_1,
       adp.accounting_period
     FROM accts_depts_periods adp
     LEFT JOIN cost_category cc
       ON adp.unique_account_name = cc.unique_account_name

)

SELECT *
FROM accts_depts_periods_cost
ORDER BY
  fiscal_year DESC,
  accounting_period,
  account_number,
  parent_department_name,
  department_name
