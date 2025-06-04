WITH transactions AS (

     SELECT *
     FROM {{ref('netsuite_transactions_source')}}

), transaction_lines AS (

     SELECT *
     FROM {{ref('netsuite_transaction_lines_xf')}}

), accounting_periods AS (

     SELECT *
     FROM {{ref('netsuite_accounting_periods')}}

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

), opex_cogs AS (

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
    WHERE a.account_number between '5000' and '6999'
      AND ap.fiscal_calendar_id = 2
      AND e.to_subsidiary_id = 1
    {{ dbt_utils.group_by(n=23) }}

), income_statement_grouping AS (

    SELECT
      oc.transaction_id,
      oc.external_ref_number,
      oc.transaction_ext_id,
      oc.document_id,
      oc.account_id,
      oc.account_name,
      oc.account_full_name,
      oc.account_number || ' - ' || oc.account_name          AS unique_account_name,
      oc.account_number,
      oc.parent_account_number,
      oc.unique_account_number,
      oc.actual_amount,
      CASE
        WHEN oc.account_number BETWEEN '5000' AND '5999'
          THEN '2-cost of sales'
        WHEN oc.account_number BETWEEN '6000' AND '6999'
          THEN '3-expense'
      END                                                    AS income_statement_grouping,
      oc.transaction_lines_memo,
      oc.entity_name,
      oc.receipt_url,
      oc.status,
      oc.transaction_type,
      oc.class_id,
      oc.class_name,
      oc.department_id,
      oc.department_name,
      oc.parent_department_name,
      oc.accounting_period_id,
      oc.accounting_period,
      oc.accounting_period_name,
      dd.fiscal_year,
      dd.fiscal_quarter,
      dd.fiscal_quarter_name
    FROM opex_cogs oc
    LEFT JOIN date_details dd
      ON dd.first_day_of_month = oc.accounting_period

), cost_category_grouping AS (

    SELECT
      isg.*,
      cc.cost_category_level_1,
      cc.cost_category_level_2
    FROM income_statement_grouping isg
    LEFT JOIN cost_category cc
      ON isg.unique_account_name = cc.unique_account_name

)

SELECT *
FROM cost_category_grouping
