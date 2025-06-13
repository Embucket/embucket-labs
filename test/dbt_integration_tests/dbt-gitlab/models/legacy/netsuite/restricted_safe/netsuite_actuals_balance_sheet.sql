{% set net_income_retained_earnings = ('income','other income','expense','other expense','other income','cost of goods sold') %}

WITH accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

), accounting_books AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_books') }}

), accounting_periods AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_periods') }}

), classes AS (

     SELECT *
     FROM {{ref('netsuite_classes')}}

), consolidated_exchange_rates AS (

     SELECT *
     FROM {{ ref('netsuite_consolidated_exchange_rates') }}

), date_details AS (

     SELECT DISTINCT
       first_day_of_month,
       fiscal_year,
       fiscal_quarter,
       fiscal_quarter_name
     FROM {{ ref('date_details') }}

), departments AS (

     SELECT *
     FROM {{ref('netsuite_departments_xf')}}

), subsidiaries AS (

     SELECT *
     FROM {{ ref('netsuite_subsidiaries_source') }}

), transactions AS (

     SELECT *
     FROM {{ ref('netsuite_transactions_source') }}

), transaction_lines AS (

     SELECT *
     FROM {{ ref('netsuite_transaction_lines_xf') }}

), period_exchange_rate_map AS ( -- exchange rates used, by accounting period, to convert to parent subsidiary

     SELECT
       consolidated_exchange_rates.accounting_period_id,
       consolidated_exchange_rates.average_rate,
       consolidated_exchange_rates.current_rate,
       consolidated_exchange_rates.historical_rate,
       consolidated_exchange_rates.from_subsidiary_id,
       consolidated_exchange_rates.to_subsidiary_id
     FROM consolidated_exchange_rates
     WHERE consolidated_exchange_rates.to_subsidiary_id IN (
       SELECT
        subsidiary_id
       FROM subsidiaries
       WHERE parent_id IS NULL  -- constrait - only the primary subsidiary has no parent
       )
       AND consolidated_exchange_rates.accounting_book_id IN (
         SELECT
           accounting_book_id
         FROM accounting_books
         WHERE is_primary = true
         )

), account_period_exchange_rate_map AS ( -- account table with exchange rate details by accounting period

     SELECT
       period_exchange_rate_map.accounting_period_id,
       period_exchange_rate_map.from_subsidiary_id,
       period_exchange_rate_map.to_subsidiary_id,
       accounts.account_id,
       CASE
         WHEN LOWER(accounts.general_rate_type) = 'historical'
           THEN period_exchange_rate_map.historical_rate
         WHEN LOWER(accounts.general_rate_type) = 'current'
           THEN period_exchange_rate_map.current_rate
         WHEN LOWER(accounts.general_rate_type) = 'average'
           THEN period_exchange_rate_map.average_rate
         ELSE NULL
       END                AS exchange_rate
     FROM accounts
     CROSS JOIN period_exchange_rate_map

), transaction_lines_w_accounting_period AS ( -- transaction line totals, by accounts, accounting period and subsidiary

     SELECT
       transaction_lines.transaction_id,
       transaction_lines.transaction_line_id,
       transaction_lines.memo,
       transaction_lines.entity_name,
       transactions.document_id,
       transactions.transaction_type,
       transaction_lines.subsidiary_id,
       transaction_lines.account_id,
       transaction_lines.class_id,
       transaction_lines.department_id,
       transactions.accounting_period_id                AS transaction_accounting_period_id,
       COALESCE(transaction_lines.amount, 0)            AS unconverted_amount
     FROM transaction_lines
     INNER JOIN transactions
       ON transaction_lines.transaction_id = transactions.transaction_id
     WHERE LOWER(transactions.transaction_type) != 'revenue arrangement'

), period_id_list_to_current_period AS ( -- period ids with all future period ids.  this is needed to calculate cumulative totals by correct exchange rates.

    SELECT
      base.accounting_period_id,
      array_agg(multiplier.accounting_period_id) WITHIN GROUP (ORDER BY multiplier.accounting_period_id) AS accounting_periods_to_include_for
    FROM accounting_periods AS base
    INNER JOIN accounting_periods AS multiplier
      ON base.accounting_period_starting_date <= multiplier.accounting_period_starting_date
      AND base.is_quarter = multiplier.is_quarter
      AND base.is_year = multiplier.is_year
      AND base.fiscal_calendar_id = multiplier.fiscal_calendar_id
      AND multiplier.accounting_period_starting_date <= CURRENT_TIMESTAMP()
    WHERE base.is_quarter = false
      AND base.is_year = false
      AND base.fiscal_calendar_id = (SELECT
                                       fiscal_calendar_id
                                     FROM subsidiaries
                                     WHERE parent_id IS NULL) -- fiscal calendar will align with parent subsidiary's default calendar
    {{ dbt_utils.group_by(n=1) }}

), flatten_period_id_array AS (

     SELECT
       accounting_period_id,
       reporting_accounting_period_id.value AS reporting_accounting_period_id
     FROM period_id_list_to_current_period,
     lateral flatten (input => accounting_periods_to_include_for) reporting_accounting_period_id
     WHERE array_size(accounting_periods_to_include_for) > 1

), transactions_in_every_calculation_period AS (

     SELECT
       transaction_lines_w_accounting_period.*,
       reporting_accounting_period_id
     FROM transaction_lines_w_accounting_period
     INNER JOIN flatten_period_id_array
       ON flatten_period_id_array.accounting_period_id = transaction_lines_w_accounting_period.transaction_accounting_period_id

), transactions_in_every_calculation_period_w_exchange_rates AS (

     SELECT
       transactions_in_every_calculation_period.*,
       exchange_reporting_period.exchange_rate    AS exchange_reporting_period,
       exchange_transaction_period.exchange_rate  AS exchange_transaction_period
     FROM transactions_in_every_calculation_period
     LEFT JOIN account_period_exchange_rate_map AS exchange_reporting_period
       ON transactions_in_every_calculation_period.account_id = exchange_reporting_period.account_id
       AND transactions_in_every_calculation_period.reporting_accounting_period_id = exchange_reporting_period.accounting_period_id
       AND transactions_in_every_calculation_period.subsidiary_id = exchange_reporting_period.from_subsidiary_id
     LEFT JOIN account_period_exchange_rate_map AS exchange_transaction_period
       ON transactions_in_every_calculation_period.account_id = exchange_transaction_period.account_id
       AND transactions_in_every_calculation_period.transaction_accounting_period_id = exchange_transaction_period.accounting_period_id
       AND transactions_in_every_calculation_period.subsidiary_id = exchange_transaction_period.from_subsidiary_id

), transactions_with_converted_amounts AS (

     SELECT
       transactions_in_every_calculation_period_w_exchange_rates.*,
       unconverted_amount * exchange_transaction_period   AS converted_amount_using_transaction_accounting_period,
       unconverted_amount * exchange_reporting_period     AS converted_amount_using_reporting_month
     FROM transactions_in_every_calculation_period_w_exchange_rates

), balance_sheet AS (

     SELECT
       transactions_with_converted_amounts.document_id,
       transactions_with_converted_amounts.memo,
       transactions_with_converted_amounts.entity_name,
       departments.parent_department_name,
       departments.department_name,
       classes.class_name,
       transactions_with_converted_amounts.transaction_type,
       reporting_accounting_periods.accounting_period_id,
       reporting_accounting_periods.accounting_period_starting_date::DATE   AS accounting_period,
       reporting_accounting_periods.accounting_period_name,
       accounts.is_account_inactive,
       CASE
         WHEN (LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           AND reporting_accounting_periods.year_id = transaction_accounting_periods.year_id)
             THEN 'net income'
         WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           THEN 'retained earnings'
         WHEN accounts.account_number = '3000'
           THEN 'retained earnings'
         ELSE LOWER(accounts.account_name)
       END                                                                  AS account_name,
       CASE
         WHEN (LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           AND reporting_accounting_periods.year_id = transaction_accounting_periods.year_id)
           THEN 'net income'
         WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           THEN 'retained earnings'
         WHEN accounts.account_number = '3000'
           THEN 'retained earnings'
         WHEN accounts.account_number = '1351'
           THEN 'other current asset'
         ELSE LOWER(accounts.account_type)
       END                                                                  AS account_type,
       CASE
         WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           THEN NULL
         WHEN accounts.account_number = '3000'
           THEN NULL
         ELSE accounts.account_id
       END                                                                  AS account_id,
       CASE
         WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           THEN ''
         WHEN accounts.account_number = '3000'
           THEN ''
         ELSE accounts.account_number
       END                                                                  AS account_number,
       CASE
         WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
           THEN ''
         WHEN accounts.account_number = '3000'
           THEN ''
         ELSE accounts.unique_account_number
       END                                                                  AS unique_account_number,
       SUM(CASE
             WHEN LOWER(accounts.account_type) IN {{net_income_retained_earnings}}
               THEN -converted_amount_using_transaction_accounting_period
             WHEN accounts.account_number IN ('3000','3014.1')
               THEN -converted_amount_using_transaction_accounting_period
             WHEN (LOWER(accounts.general_rate_type) = 'historical' AND accounts.is_leftside_account = false)
               THEN -converted_amount_using_transaction_accounting_period
             WHEN (LOWER(accounts.general_rate_type) = 'historical' AND accounts.is_leftside_account = true)
               THEN converted_amount_using_transaction_accounting_period
             WHEN (accounts.is_balancesheet_account = true AND accounts.is_leftside_account = false)
               THEN -converted_amount_using_reporting_month
             WHEN (accounts.is_balancesheet_account = true AND accounts.is_leftside_account = true)
               THEN converted_amount_using_reporting_month
             ELSE 0
           END)                                                             AS actual_amount
       FROM  transactions_with_converted_amounts
       LEFT JOIN accounts
         ON transactions_with_converted_amounts.account_id = accounts.account_id
       LEFT JOIN classes
         ON transactions_with_converted_amounts.class_id = classes.class_id
       LEFT JOIN departments
         ON transactions_with_converted_amounts.department_id = departments.department_id
       LEFT JOIN accounting_periods AS reporting_accounting_periods
         ON transactions_with_converted_amounts.reporting_accounting_period_id = reporting_accounting_periods.accounting_period_id
       LEFT JOIN accounting_periods AS transaction_accounting_periods
         ON transactions_with_converted_amounts.transaction_accounting_period_id = transaction_accounting_periods.accounting_period_id
       WHERE reporting_accounting_periods.fiscal_calendar_id    = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND transaction_accounting_periods.fiscal_calendar_id  = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND LOWER(accounts.account_type) != 'statistical'
         AND accounts.account_number != '3035'
        {{ dbt_utils.group_by(n=16) }}

       UNION ALL

       SELECT
         transactions_with_converted_amounts.document_id,
         transactions_with_converted_amounts.memo,
         transactions_with_converted_amounts.entity_name,
         departments.parent_department_name,
         departments.department_name,
         classes.class_name,
         transactions_with_converted_amounts.transaction_type,
         reporting_accounting_periods.accounting_period_id,
         reporting_accounting_periods.accounting_period_starting_date::DATE   AS accounting_period,
         reporting_accounting_periods.accounting_period_name,
         accounts.is_account_inactive,
         'Cumulative Translation Adjustment'                                  AS account_name,
         'Cumulative Translation Adjustment'                                  AS account_type,
         NULL                                                                 AS account_id,
         ''                                                                   AS account_number,
         NULL                                                                 AS unique_account_number,
         SUM(CASE
               WHEN LOWER(account_type) IN {{net_income_retained_earnings}}
                 THEN converted_amount_using_transaction_accounting_period
               WHEN LOWER(account_type) IN ('equity', 'retained earnings', 'net income')
                 THEN converted_amount_using_transaction_accounting_period
               ELSE converted_amount_using_reporting_month
             END)                                                             AS actual_amount
       FROM  transactions_with_converted_amounts
       LEFT JOIN accounts
         ON transactions_with_converted_amounts.account_id = accounts.account_id
       LEFT JOIN classes
         ON transactions_with_converted_amounts.class_id = classes.class_id
       LEFT JOIN departments
         ON transactions_with_converted_amounts.department_id = departments.department_id
       LEFT JOIN accounting_periods AS reporting_accounting_periods
         ON transactions_with_converted_amounts.reporting_accounting_period_id = reporting_accounting_periods.accounting_period_id
       LEFT JOIN accounting_periods AS transaction_accounting_periods
         ON transactions_with_converted_amounts.transaction_accounting_period_id = transaction_accounting_periods.accounting_period_id
       WHERE reporting_accounting_periods.fiscal_calendar_id    = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND transaction_accounting_periods.fiscal_calendar_id  = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND LOWER(accounts.account_type) != 'statistical'
         AND accounts.account_number != '3035'
         {{ dbt_utils.group_by(n=11) }}

), balance_sheet_grouping AS (

      SELECT
        document_id,
        memo,
        entity_name,
        transaction_type,
        account_id,
        account_name,
        account_number,
        unique_account_number,
        account_number || ' - ' || account_name   AS unique_account_name,
        account_type,
        parent_department_name,
        department_name,
        class_name,
        CASE
          WHEN account_type IN ('accounts receivable','bank','other current asset','unbilled receivable','deferred expense')
            THEN '1-current assets'
          WHEN account_type IN ('accounts payable','credit card','deferred revenue','other current liability')
            THEN '1-current liabilities'
          WHEN account_type IN ('fixed asset')
            THEN '3-fixed assets'
          WHEN account_type IN ('long term liability')
            THEN '2-long term liabilities'
          WHEN account_type IN ('other asset')
            THEN '2-other assets'
          WHEN account_type IN ('net income','retained earnings','equity','Cumulative Translation Adjustment')
            THEN '3-equity'
          ELSE 'need classification'
        END                                       AS balance_sheet_grouping_level_2,
        CASE
          WHEN account_type IN ('accounts receivable','bank','other current asset','unbilled receivable','fixed asset','other asset','deferred expense')
            THEN '1-assets'
          WHEN account_type IN ('accounts payable','credit card','deferred revenue','other current liability',
                                'equity','long term liability','net income','retained earnings','Cumulative Translation Adjustment')
            THEN '2-liabilities & equity'
          ELSE 'need classification'
        END                                       AS balance_sheet_grouping_level_3,
        is_account_inactive,
        actual_amount,
        accounting_period_id,
        accounting_period,
        accounting_period_name,
        fiscal_year,
        fiscal_quarter,
        fiscal_quarter_name

      FROM balance_sheet b
      LEFT JOIN date_details d
        ON b.accounting_period = d.first_day_of_month

)

SELECT *
FROM balance_sheet_grouping
ORDER BY accounting_period, account_name
