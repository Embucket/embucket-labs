WITH transactions AS (

     SELECT *
     FROM {{ref('netsuite_transactions_source')}}

), transaction_lines AS (

     SELECT *
     FROM {{ref('netsuite_transaction_lines_xf')}}

), accounting_periods AS (

     SELECT *
     FROM {{ref('netsuite_accounting_periods_source')}}

), accounts AS (

     SELECT *
     FROM {{ref('netsuite_accounts_xf')}}

), classes AS (

     SELECT *
     FROM {{ref('netsuite_classes')}}

), subsidiaries AS (

     SELECT *
     FROM {{ref('netsuite_subsidiaries_source')}}

), departments AS (

     SELECT *
     FROM {{ref('netsuite_departments_xf')}}

), consolidated_exchange_rates AS (

     SELECT *
     FROM {{ref('netsuite_consolidated_exchange_rates')}}

), date_details AS (

     SELECT DISTINCT
       first_day_of_month,
       fiscal_year,
       fiscal_quarter,
       fiscal_quarter_name
     FROM {{ref('date_details')}}

), cost_category AS (

     SELECT *
     FROM {{ref('netsuite_expense_cost_category')}}

), income AS (

     SELECT
       t.transaction_id,
       t.external_ref_number,
       t.transaction_ext_id,
       t.document_id,
       tl.memo                                          AS transaction_lines_memo,
       tl.entity_name,
       tl.receipt_url,
       t.status,
       t.transaction_type,
       a.account_id,
       a.account_name,
       a.account_full_name,
       a.account_number,
       a.unique_account_number,
       a.parent_account_number,
       cl.class_id,
       cl.class_name,
       d.department_id,
       d.department_name,
       d.parent_department_name,
       ap.accounting_period_id,
       ap.accounting_period_starting_date::DATE         AS accounting_period,
       ap.accounting_period_name,
       SUM(CASE WHEN tl.subsidiary_id = 1 THEN amount
                ELSE (tl.amount * e.average_rate) END)  AS actual_amount
    FROM transaction_lines tl
    LEFT JOIN transactions t
      ON tl.transaction_id = t.transaction_id
    LEFT JOIN accounts a
      ON a.account_id = tl.account_id
    LEFT JOIN classes cl
      ON tl.class_id = cl.class_id
    LEFT JOIN departments d
      ON d.department_id = tl.department_id
    LEFT JOIN accounting_periods ap
      ON ap.accounting_period_id = t.accounting_period_id
    LEFT JOIN subsidiaries s
      ON tl.subsidiary_id = s.subsidiary_id
    LEFT JOIN consolidated_exchange_rates e
      ON ap.accounting_period_id = e.accounting_period_id
      AND e.from_subsidiary_id = s.subsidiary_id
    WHERE a.account_number between '4000' and '4999'
      AND ap.fiscal_calendar_id = 2
      AND e.to_subsidiary_id = 1
    {{ dbt_utils.group_by(n=23) }}

), income_statement_grouping AS (

    SELECT
      i.transaction_id,
      i.external_ref_number,
      i.transaction_ext_id,
      i.document_id,
      i.account_id,
      i.account_name,
      i.account_full_name,
      i.account_number || ' - ' || i.account_name          AS unique_account_name,
      i.account_number,
      i.parent_account_number,
      i.unique_account_number,
      -(i.actual_amount)                                   AS actual_amount,
      CASE
        WHEN i.account_number BETWEEN '4000' AND '4999'
          THEN '1-income'
      END                                                  AS income_statement_grouping,
      i.transaction_lines_memo,
      i.entity_name,
      i.receipt_url,
      i.status,
      i.transaction_type,
      i.class_id,
      i.class_name,
      i.department_id,
      i.department_name,
      i.parent_department_name,
      i.accounting_period_id,
      i.accounting_period,
      i.accounting_period_name,
      dd.fiscal_year,
      dd.fiscal_quarter,
      dd.fiscal_quarter_name
    FROM income i
    LEFT JOIN date_details dd
      ON dd.first_day_of_month = i.accounting_period

), cost_category_grouping AS (

    SELECT
      isg.*,
      'N/A'                                            AS cost_category_level_1,
      'N/A'                                            AS cost_category_level_2
    FROM income_statement_grouping isg

)

SELECT *
FROM cost_category_grouping
