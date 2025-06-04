{{ simple_cte([
    ('prep_crm_case', 'prep_crm_case'),
    ('dim_crm_user', 'dim_crm_user'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account_daily_snapshot', 'dim_crm_account_daily_snapshot'),
    ('dim_product_detail', 'dim_product_detail'),
    ('mart_charge', 'mart_charge'),
    ('mart_product_usage_paid_user_metrics_monthly', 'mart_product_usage_paid_user_metrics_monthly'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr','mart_arr'),
    ('mart_crm_account','mart_crm_account')
    ])

}},
--
usage_data AS (
  SELECT
    rpt_product_usage_health_score.snapshot_month,
    rpt_product_usage_health_score.dim_crm_account_id,
    rpt_product_usage_health_score.crm_account_name,
    MIN(rpt_product_usage_health_score.scm_score)                              AS scm_score,
    MIN(rpt_product_usage_health_score.ci_score)                               AS ci_score,
    MIN(mart_product_usage_paid_user_metrics_monthly.epics_28_days_user)       AS epics_28_days_user,
    MIN(mart_product_usage_paid_user_metrics_monthly.issues_28_days_user)      AS issues_28_days_user,
    MIN(mart_product_usage_paid_user_metrics_monthly.issues_edit_28_days_user) AS issues_edit_28_days_user,
    MIN(mart_product_usage_paid_user_metrics_monthly.epics_usage_28_days_user) AS epics_usage_28_days_user
  FROM rpt_product_usage_health_score
  LEFT JOIN mart_product_usage_paid_user_metrics_monthly ON rpt_product_usage_health_score.primary_key = mart_product_usage_paid_user_metrics_monthly.primary_key
  WHERE rpt_product_usage_health_score.snapshot_month > '2023-01-30'
  GROUP BY 1, 2, 3
),

first_order AS (
  SELECT DISTINCT
    mart_crm_opportunity.dim_parent_crm_account_id,
    LAST_VALUE(mart_crm_opportunity.net_arr) OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)    AS net_arr,
    LAST_VALUE(mart_crm_opportunity.close_date) OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC) AS close_date,
    LAST_VALUE(dim_date.fiscal_year) OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)            AS fiscal_year,
    LAST_VALUE(mart_crm_opportunity.sales_qualified_source_name)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)                                           AS sqs
  FROM mart_crm_opportunity
  LEFT JOIN dim_date
    ON mart_crm_opportunity.close_date = dim_date.date_actual
  WHERE mart_crm_opportunity.is_won
    AND mart_crm_opportunity.order_type = '1. New - First Order'
),

latest_churn AS (
  SELECT
    snapshot_date                                                                               AS close_date,
    dim_crm_account_id,
    carr_this_account,
    LAG(carr_this_account, 1) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC) AS prior_carr,
    -prior_carr                                                                                 AS net_arr
    ,
    MAX(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END) OVER (PARTITION BY dim_crm_account_id)                                                 AS last_carr_date
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date >= '2019-02-01'
    AND snapshot_date = DATE_TRUNC('month', snapshot_date)
  -- and dim_crm_account_id = '0014M00001lbBt1QAE'
  QUALIFY
    prior_carr > 0
    AND carr_this_account = 0
    AND snapshot_date > last_carr_date
),

high_value_case_prep AS (
  SELECT
    prep_crm_case.dim_crm_account_id,
    prep_crm_case.case_id,
    prep_crm_case.dim_crm_user_id,
    prep_crm_case.subject,
    dim_crm_user.user_name                                                 AS case_owner_name,
    dim_crm_user.department                                                AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name                                            AS case_user_role_name,
    dim_crm_user.user_role_type,
    prep_crm_case.created_date,
    MAX(prep_crm_case.created_date) OVER (PARTITION BY prep_crm_case.dim_crm_account_id) AS last_high_value_date
  FROM prep_crm_case
  LEFT JOIN dim_crm_user
    ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  WHERE prep_crm_case.record_type_id IN ('0128X000001pPRkQAM')
    --    and account_id = '0014M00001gTGESQA4'
    AND LOWER(prep_crm_case.subject) LIKE '%high value account%'
    AND LOWER(prep_crm_case.subject) LIKE 'fy%'
),

high_value_case AS (
  SELECT *
  FROM high_value_case_prep
  WHERE created_date = last_high_value_date
),

-- start_values AS ( -- This is a large slow to query table
--   SELECT
--     dim_crm_account_id,
--     carr_account_family,
--     carr_this_account,
--     parent_crm_account_lam_dev_count,
--     pte_score,
--     ptc_score
--   FROM dim_crm_account_daily_snapshot
--   WHERE snapshot_date = '2025-02-10' -----placeholder date for start of year
-- ),

-- first_high_value_case_prep AS (
--   SELECT
--     prep_crm_case.dim_crm_account_id,
--     prep_crm_case.case_id,
--     prep_crm_case.dim_crm_user_id,
--     prep_crm_case.subject,
--     dim_crm_user.user_name                                                 AS case_owner_name,
--     dim_crm_user.department                                                AS case_department,
--     dim_crm_user.team,
--     dim_crm_user.manager_name,
--     dim_crm_user.user_role_name                                            AS case_user_role_name,
--     dim_crm_user.user_role_type,
--     prep_crm_case.created_date,
--     MIN(prep_crm_case.created_date) OVER (PARTITION BY prep_crm_case.dim_crm_account_id) AS first_high_value_date
--   FROM prep_crm_case
--   LEFT JOIN dim_crm_user
--     ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
--   WHERE prep_crm_case.record_type_id IN ('0128X000001pPRkQAM')
--     AND LOWER(prep_crm_case.subject) LIKE '%high value account%'
-- ), -----subject placeholder - this could change

-- first_high_value_case AS (
--   SELECT *
--   FROM first_high_value_case_prep
--   WHERE created_date = first_high_value_date
-- ),

bronze_starter_accounts AS (
  SELECT 
  DISTINCT
  dim_crm_account_id
  --,product_rate_plan_name
  FROM
    mart_arr
  WHERE arr_month >= '2020-02-01'
    AND arr_month <= '2022-02-01'
    AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
),

eoa_cohorts_prep AS (
  SELECT
    mart_arr.arr_month,
    --,ping_created_at
    mart_arr.subscription_end_month,
    mart_arr.dim_crm_account_id,
    mart_arr.crm_account_name,
    --,MART_CRM_ACCOUNT.CRM_ACCOUNT_OWNER
    mart_arr.dim_subscription_id,
    mart_arr.dim_subscription_id_original,
    mart_arr.subscription_name,
    mart_arr.subscription_sales_type,
    mart_arr.auto_pay,
    mart_arr.default_payment_method_type,
    mart_arr.contract_auto_renewal,
    mart_arr.turn_on_auto_renewal,
    mart_arr.turn_on_cloud_licensing,
    mart_arr.contract_seat_reconciliation,
    mart_arr.turn_on_seat_reconciliation,
    COALESCE(mart_arr.contract_seat_reconciliation = 'Yes' AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE) AS qsr_enabled_flag,
    mart_arr.product_tier_name,
    mart_arr.product_delivery_type,
    mart_arr.product_rate_plan_name,
    mart_arr.arr,
    --,monthly_mart.MAX_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
    (mart_arr.arr / NULLIFZERO(mart_arr.quantity))                                                                  AS arr_per_user,
    arr_per_user / 12                                                                                               AS monthly_price_per_user,
    mart_arr.mrr / NULLIFZERO(mart_arr.quantity)                                                                    AS mrr_check
  FROM mart_arr
-- LEFT JOIN RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_ACCOUNT
--     ON mart_arr.DIM_CRM_ACCOUNT_ID = MART_CRM_ACCOUNT.DIM_CRM_ACCOUNT_ID
  INNER JOIN bronze_starter_accounts
    ON mart_arr.dim_crm_account_id = bronze_starter_accounts.dim_crm_account_id
  WHERE mart_arr.arr_month = '2023-01-01'
    AND mart_arr.product_tier_name LIKE '%Premium%'
    AND ((
      monthly_price_per_user >= 14
      AND monthly_price_per_user <= 16
    ) OR (monthly_price_per_user >= 7.5 AND monthly_price_per_user <= 9.5
    ))
),

eoa_cohorts AS (
  SELECT DISTINCT dim_crm_account_id
  FROM eoa_cohorts_prep
),

free_promo AS (
  SELECT DISTINCT dim_crm_account_id
  FROM mart_charge
  WHERE subscription_start_date >= '2023-02-01'
    AND rate_plan_charge_description = 'fo-discount-70-percent'
),

price_increase_prep AS (
  SELECT
    charge.* EXCLUDE created_by,
    charge.arr / NULLIFZERO(charge.quantity)     AS actual_price,
    prod.annual_billing_list_price AS list_price
  FROM mart_charge AS charge
  INNER JOIN dim_product_detail AS prod
    ON charge.dim_product_detail_id = prod.dim_product_detail_id
  WHERE charge.subscription_start_date >= '2023-04-01'
    AND charge.subscription_start_date <= '2023-07-01'
    AND charge.type_of_arr_change = 'New'
    AND charge.quantity > 0
    AND actual_price > 228
    AND actual_price < 290
    AND charge.rate_plan_charge_name LIKE '%Premium%'
),

price_increase AS (
  SELECT DISTINCT dim_crm_account_id
  FROM price_increase_prep
),

ultimate AS (
  SELECT DISTINCT
    dim_parent_crm_account_id,
    arr_month,
    MAX(arr_month) OVER (PARTITION BY dim_parent_crm_account_id ORDER BY arr_month DESC) AS last_ultimate_arr_month
  FROM mart_arr
  WHERE product_tier_name LIKE '%Ultimate%'
    AND arr > 0
),

amer_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('AMER SMB Sales')
    OR owner_role = 'Advocate_SMB_AMER'
),

emea_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('EMEA SMB Sales', 'APJ SMB Sales')
    OR owner_role = 'Advocate_SMB_EMEA'
),

account_base AS (
  SELECT
    acct.* EXCLUDE (model_created_date, model_updated_date, created_by, updated_by, dbt_updated_at, dbt_created_at),
    CASE
      WHEN (acct.snapshot_date >= '2024-02-01' AND amer_accounts.dim_crm_account_id IS NOT NULL)
        OR (
          acct.snapshot_date < '2024-02-01'
          AND (acct.parent_crm_account_geo IN ('AMER') OR acct.parent_crm_account_region IN ('AMER'))
        )
        THEN 'AMER'
      WHEN
        (acct.snapshot_date >= '2024-02-01' AND emea_accounts.dim_crm_account_id IS NOT NULL)
        OR (acct.snapshot_date < '2024-02-01' AND (acct.parent_crm_account_geo IN ('EMEA', 'APJ', 'APAC') OR acct.parent_crm_account_region IN ('EMEA', 'APJ', 'APAC')))
        THEN 'EMEA'
      ELSE 'Other'
    END                                                             AS team,
    COALESCE(
      (acct.snapshot_date >= '2024-02-01' AND (emea_accounts.dim_crm_account_id IS NOT NULL OR amer_accounts.dim_crm_account_id IS NOT NULL))
      OR
      (
        acct.snapshot_date < '2024-02-01'
        AND (acct.carr_account_family <= 30000)
        AND (acct.parent_crm_account_max_family_employee <= 100 OR acct.parent_crm_account_max_family_employee IS NULL)
        AND ultimate.dim_parent_crm_account_id IS NULL
        AND acct.parent_crm_account_sales_segment IN ('SMB', 'Mid-Market', 'Large')
        AND acct.parent_crm_account_upa_country != 'JP'
        AND acct.is_jihu_account = FALSE
      ), FALSE
    )                                                               AS gds_account_flag,
    first_order.fiscal_year                                         AS fo_fiscal_year,
    first_order.close_date                                          AS fo_close_date,
    first_order.net_arr                                             AS fo_net_arr,
    first_order.sqs                                                 AS fo_sqs,
    --   first_order.fo_opp_name,
    churn.close_date                                                AS churn_close_date,
    churn.net_arr                                                   AS churn_net_arr,
    NOT COALESCE(fo_fiscal_year <= 2024, FALSE)                     AS new_fy25_fo_flag,
    high_value_case.case_owner_name                                 AS high_value_case_owner,
    high_value_case.dim_crm_user_id                                 AS high_value_case_owner_id,
    high_value_case.case_id                                         AS high_value_case_id,
    high_value_case.team                                            AS high_value_case_owner_team,
    high_value_case.manager_name                                    AS high_value_case_owner_manager,
    COALESCE(eoa_cohorts.dim_crm_account_id IS NOT NULL, FALSE)     AS eoa_flag,
    COALESCE(free_promo.dim_crm_account_id IS NOT NULL, FALSE)      AS free_promo_flag,
    COALESCE(price_increase.dim_crm_account_id IS NOT NULL, FALSE)  AS price_increase_promo_flag,
    COALESCE(ultimate.dim_parent_crm_account_id IS NOT NULL, FALSE) AS ultimate_customer_flag,
    usage_data.* EXCLUDE (dim_crm_account_id, crm_account_name, snapshot_month)
  FROM dim_crm_account_daily_snapshot AS acct
  ------subquery that gets latest FO data
  LEFT JOIN first_order
    ON acct.dim_parent_crm_account_id = first_order.dim_parent_crm_account_id
  ---------subquery that gets latest churn data
  LEFT JOIN latest_churn AS churn
    ON acct.dim_crm_account_id = churn.dim_crm_account_id
  --------subquery to get high tier case owner
  LEFT JOIN high_value_case
    ON acct.dim_crm_account_id = high_value_case.dim_crm_account_id
  --------------subquery to get start of FY25 values
  -- LEFT JOIN start_values
  --   ON acct.dim_crm_account_id = start_values.dim_crm_account_id
  -- -----subquery to get FIRST high value case
  -- LEFT JOIN first_high_value_case
  --   ON acct.dim_crm_account_id = first_high_value_case.dim_crm_account_id
  -----EOA cohort accounts
  LEFT JOIN eoa_cohorts
    ON acct.dim_crm_account_id = eoa_cohorts.dim_crm_account_id
  ------free limit promo cohort accounts
  LEFT JOIN free_promo
    ON acct.dim_crm_account_id = free_promo.dim_crm_account_id
  ------price increase promo cohort accounts
  LEFT JOIN price_increase
    ON acct.dim_crm_account_id = price_increase.dim_crm_account_id
  LEFT JOIN ultimate
    ON acct.dim_parent_crm_account_id = ultimate.dim_parent_crm_account_id
      AND (ultimate.arr_month = DATE_TRUNC('month', acct.snapshot_date)
      OR ultimate.last_ultimate_arr_month = dateadd('month', -1, DATE_TRUNC('month', acct.snapshot_date)))
  ----- amer and apac accounts
  LEFT JOIN amer_accounts
    ON acct.dim_crm_account_id = amer_accounts.dim_crm_account_id
  ----- emea emea accounts
  LEFT JOIN emea_accounts
    ON acct.dim_crm_account_id = emea_accounts.dim_crm_account_id
  ----- bring in Agile and other usage data
  LEFT JOIN usage_data
    ON usage_data.dim_crm_account_id = acct.dim_crm_account_id
    -------filtering to get current account data
      AND acct.snapshot_date = usage_data.snapshot_month
  WHERE acct.snapshot_date >= '2023-02-01'

)

{{ dbt_audit(
    cte_ref="account_base",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2025-02-17"
) }}
