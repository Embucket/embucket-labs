
{{ simple_cte([
    ('prep_crm_case', 'prep_crm_case'),
    ('dim_crm_user', 'dim_crm_user'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_arr', 'mart_arr'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account_daily_snapshot', 'dim_crm_account_daily_snapshot'),
    ('dim_product_detail', 'dim_product_detail'),
    ('mart_charge', 'mart_charge'),
    ('mart_crm_account', 'mart_crm_account'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_billing_account', 'dim_billing_account'),
    ('mart_product_usage_paid_user_metrics_monthly', 'mart_product_usage_paid_user_metrics_monthly'),
    ('dim_subscription_snapshot_bottom_up', 'dim_subscription_snapshot_bottom_up'),
    ('mart_crm_task', 'mart_crm_task'),
    ('prep_billing_account_user','prep_billing_account_user'),
    ('sfdc_contact_snapshots_source','sfdc_contact_snapshots_source'),
    ('case_data','gds_case_inputs'),
    ('zuora_subscription_source', 'zuora_subscription_source')
    ]) 

}},

--Pulls all current High Value account cases
high_value_case_one AS (
  SELECT DISTINCT
    dim_crm_account_id,
    ------- placeplacer until confirmation of case name 
    COALESCE(
      LOWER(subject) = 'high value account check in'
      OR CONTAINS(LOWER(case_type), 'inbound request')
      OR (LOWER(subject) = 'fy25 high value account' AND status IN ('Open', 'In Progress') AND DATEDIFF('day', created_date, CURRENT_DATE) <= 90),
      FALSE
    ) AS high_value_last_90
  FROM prep_crm_case
  WHERE (
    (DATEDIFF('day', created_date, CURRENT_DATE) <= 90 AND status = 'Closed: Unresponsive')
    OR (DATEDIFF('day', closed_date, CURRENT_DATE) <= 90 AND status = 'Closed: Resolved')
    OR status IN ('Open', 'In Progress')
  )
  AND record_type_id IN ('0128X000001pPRkQAM')
),

--Identifies accounts with a High Value case in last 90 days
high_value_case_last_90 AS (
  SELECT DISTINCT
    dim_crm_account_id,
    TRUE AS high_value_last_90
  FROM high_value_case_one
  WHERE high_value_last_90 = TRUE
),

--Identifies accounts with any cases in the last 90 days
any_case_last_90 AS (
  SELECT DISTINCT
    dim_crm_account_id,
    TRUE AS any_case_last_90 ------- placeplacer until confirmation of case name 
  FROM prep_crm_case
  WHERE (
    (DATEDIFF('day', created_date, CURRENT_DATE) <= 90 AND status = 'Closed: Unresponsive')
    OR (DATEDIFF('day', closed_date, CURRENT_DATE) <= 90 AND status = 'Closed: Resolved')
    OR status IN ('Open', 'In Progress')
  )
  AND record_type_id IN ('0128X000001pPRkQAM')
),

--Pulls info about the most recent High Value case
-- last_high_value_one AS (
--   SELECT
--     prep_crm_case.dim_crm_account_id,
--     --opportunity_id,
--     prep_crm_case.case_id,
--     prep_crm_case.dim_crm_user_id                                                        AS owner_id, -----------check to make sure this is owner id 
--     prep_crm_case.subject,
--     dim_crm_user.user_name                                                               AS case_owner_name,
--     dim_crm_user.department                                                              AS case_department,
--     dim_crm_user.team,
--     dim_crm_user.manager_name,
--     dim_crm_user.user_role_name                                                          AS case_user_role_name,
--     dim_crm_user.user_role_type,
--     prep_crm_case.created_date,
--     MAX(prep_crm_case.created_date) OVER (PARTITION BY prep_crm_case.dim_crm_account_id) AS last_high_value_date
--   FROM prep_crm_case
--   LEFT JOIN dim_crm_user
--     ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
--   WHERE prep_crm_case.record_type_id IN ('0128X000001pPRkQAM')
--     AND LOWER(prep_crm_case.subject) LIKE '%high value account%'
-- ),


-- last_high_value_case AS (
--   SELECT *
--   FROM last_high_value_one
--   WHERE created_date = last_high_value_date
-- ),

--Identifies accounts with High Adoption cases in last 90 days
high_adoption_one AS (
  SELECT DISTINCT
    dim_crm_account_id,
    COALESCE((
      CONTAINS(LOWER(subject), 'duo trial')
      OR CONTAINS(LOWER(subject), 'high adoption')
      OR CONTAINS(LOWER(subject), 'overage')
      OR CONTAINS(LOWER(subject), 'future tier 1')
    ),
    FALSE) AS ha_last_90
  FROM prep_crm_case
  WHERE DATEDIFF('day', created_date, CURRENT_DATE) <= 90
    AND record_type_id IN ('0128X000001pPRkQAM')
),

high_adoption_last_90 AS (
  SELECT DISTINCT
    dim_crm_account_id,
    ha_last_90
  FROM high_adoption_one
  WHERE ha_last_90 = TRUE
),

--Identifies accounts with Low Adoption cases in last 180 days
low_adoption_one AS (
  SELECT DISTINCT
    dim_crm_account_id,
    COALESCE((
      CONTAINS(LOWER(subject), 'ptc')
      OR CONTAINS(LOWER(subject), 'low adoption')
      OR CONTAINS(LOWER(subject), 'underutilization')
      OR CONTAINS(LOWER(subject), 'future tier 1')
    ),
    FALSE) AS la_last_180
  FROM prep_crm_case
  WHERE DATEDIFF('day', created_date, CURRENT_DATE) <= 180
    AND record_type_id IN ('0128X000001pPRkQAM')
),


low_adoption_last_180 AS (
  SELECT DISTINCT
    dim_crm_account_id,
    TRUE AS la_last_180
  FROM low_adoption_one
  WHERE la_last_180 = TRUE
),

--Identifies accounts with at least 1 Failed QSR case
qsr_failed_one AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_crm_opportunity_id,
    COALESCE(CONTAINS(LOWER(subject), 'failed qsr'), FALSE) AS qsr_failed_last_75
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND CONTAINS(LOWER(subject), 'failed qsr')
),

qsr_failed_last_75 AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_crm_opportunity_id,
    TRUE AS qsr_failed_last_75
  FROM qsr_failed_one
  WHERE qsr_failed_last_75
),

--Retrieves the most recent case 
last_case_data_one AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dim_crm_account_id ORDER BY created_date DESC) AS rn
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND status IN ('Open', 'In Progress')
),

last_case_data AS (
  SELECT *
  FROM last_case_data_one
  WHERE rn = 1
),

--Retrieves the most recent Renewal case
last_renewal_case_data_one AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dim_crm_account_id ORDER BY created_date DESC) AS rn
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND CONTAINS(LOWER(subject), 'renew')
),

last_renewal_case_data AS (
  SELECT *
  FROM last_renewal_case_data_one
  WHERE rn = 1
),

--Gets count of all open cases on each account
open_cases AS (
  SELECT
    dim_crm_account_id,
    COUNT(DISTINCT case_id) AS count_cases
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND status IN ('Open', 'In Progress')
  GROUP BY 1
),

--Counts open renewal opportunities per account
open_renewal_opps AS (
  SELECT
    dim_crm_account_id,
    COUNT(DISTINCT dim_crm_opportunity_id) AS count_open_opps
  FROM mart_crm_opportunity
  WHERE subscription_type = 'Renewal'
    AND is_closed = FALSE
  GROUP BY 1
),

--Identifies subscriptions where autorenew will fail due to having multiple non-Storage products
auto_renew_will_fail_one AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    product_rate_plan_name,
    subscription_end_month                                                    AS auto_renewal_sub_end_month,
    turn_on_auto_renewal,
    COUNT(DISTINCT product_tier_name) OVER (PARTITION BY dim_subscription_id) AS sub_prod_count
  FROM mart_arr
  WHERE (arr_month = DATE_TRUNC('month', CURRENT_DATE()) OR arr_month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'))
    --and parent_crm_account_sales_segment = 'SMB'
    AND LOWER(product_tier_name) NOT LIKE '%storage%'
    AND turn_on_auto_renewal = 'Yes'
  QUALIFY sub_prod_count > 1
),

auto_renew_will_fail AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_subscription_id
  FROM auto_renew_will_fail_one
),

--Identifies which High Value accounts have had their cases closed due to churn
tier1_full_churns AS (
  SELECT dim_crm_account_id
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND subject = 'FY25 High Value Account'
    AND status = 'Closed: Resolved'
    AND resolution_action = 'Request Not Possible'
),

--Pulls existing Duo Trial lead cases
existing_duo_trial AS (
  SELECT dim_crm_account_id
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND subject = 'Duo Trial Started' OR (subject = 'Customer MQL' AND dim_crm_account_id IN ('0014M00001sEismQAC', '001PL000004lgERYAY'))
),

--Identifies which subscriptions have Duo on them
duo_on_sub AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_subscription_id,
    product_rate_plan_name,
    subscription_end_month                                          AS auto_renewal_sub_end_month,
    turn_on_auto_renewal,
    COALESCE(CONTAINS(LOWER(product_rate_plan_name), 'duo'), FALSE) AS duo_flag
  --arr_month,
  --max(arr_month) over(partition by dim_subscription_id) as latest_arr_month
  FROM mart_arr
  WHERE (arr_month = DATE_TRUNC('month', CURRENT_DATE()) OR arr_month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'))
    AND duo_flag = TRUE
),

--Pulls information about the most recent First Order on each account
first_order AS (
  SELECT DISTINCT
    mart_crm_opportunity.dim_parent_crm_account_id,
    LAST_VALUE(mart_crm_opportunity.net_arr)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
      AS net_arr,
    LAST_VALUE(mart_crm_opportunity.close_date)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
      AS close_date,
    LAST_VALUE(dim_date.fiscal_year)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
      AS fiscal_year,
    LAST_VALUE(mart_crm_opportunity.sales_qualified_source_name)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
      AS sqs,
    LAST_VALUE(mart_crm_opportunity.opportunity_name)
      OVER (PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
      AS fo_opp_name
  FROM mart_crm_opportunity
  LEFT JOIN dim_date
    ON mart_crm_opportunity.close_date = dim_date.date_actual
  WHERE mart_crm_opportunity.is_won
    AND mart_crm_opportunity.order_type = '1. New - First Order'
),

--Pulls information about the most recent Churn on each account
latest_churn AS (
  SELECT
    snapshot_date                                                                               AS close_date,
    dim_crm_account_id,
    carr_this_account,
    LAG(carr_this_account, 1) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC) AS prior_carr,
    -prior_carr                                                                                 AS net_arr,
    MAX(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END) OVER (PARTITION BY dim_crm_account_id)                                                 AS last_carr_date
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date >= '2019-02-01'
    AND snapshot_date = DATE_TRUNC('month', snapshot_date)
  QUALIFY
    prior_carr > 0
    AND carr_this_account = 0
    AND snapshot_date > last_carr_date
),

high_value_case_prep AS (
  SELECT
    prep_crm_case.dim_crm_account_id                                                     AS account_id,
    prep_crm_case.case_id,
    prep_crm_case.dim_crm_user_id                                                        AS owner_id, ---------check that this is the same as owner ID 
    prep_crm_case.subject,
    dim_crm_user.user_name                                                               AS case_owner_name,
    dim_crm_user.department                                                              AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name                                                          AS case_user_role_name,
    dim_crm_user.user_role_type,
    prep_crm_case.created_date,
    MAX(prep_crm_case.created_date) OVER (PARTITION BY prep_crm_case.dim_crm_account_id) AS last_high_value_date
  FROM prep_crm_case
  LEFT JOIN dim_crm_user
    ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  WHERE prep_crm_case.record_type_id IN ('0128X000001pPRkQAM')
    --    and account_id = '0014M00001gTGESQA4'
    AND LOWER(prep_crm_case.subject) LIKE '%high value account%'
),

--Duplicative - gets most recent High Value case
high_value_case AS (
  SELECT *
  FROM high_value_case_prep
  WHERE created_date = last_high_value_date
),

--Information about the account from the beginning of the fiscal year
start_values AS ( -- This is a large slow to query table
  SELECT
    dim_crm_account_id,
    carr_account_family,
    carr_this_account,
    parent_crm_account_lam_dev_count,
    pte_score,
    ptc_score
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date = '2024-02-10'
),

--First High Value case on each account (probably unneeded)
first_high_value_case_one AS (
  SELECT
    prep_crm_case.dim_crm_account_id                                                     AS account_id,
    prep_crm_case.case_id,
    prep_crm_case.dim_crm_user_id                                                        AS owner_id, ---------check that this is the same as owner ID 
    prep_crm_case.subject,
    dim_crm_user.user_name                                                               AS case_owner_name,
    dim_crm_user.department                                                              AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name                                                          AS case_user_role_name,
    dim_crm_user.user_role_type,
    prep_crm_case.created_date,
    MIN(prep_crm_case.created_date) OVER (PARTITION BY prep_crm_case.dim_crm_account_id) AS first_high_value_date
  FROM prep_crm_case
  LEFT JOIN dim_crm_user
    ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  WHERE prep_crm_case.record_type_id IN ('0128X000001pPRkQAM')
    AND LOWER(prep_crm_case.subject) LIKE '%high value account%'
),

first_high_value_case AS (
  SELECT *
  FROM first_high_value_case_one
  WHERE created_date = first_high_value_date
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
    --,monthly_mart.max_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
    DIV0(mart_arr.arr, mart_arr.quantity)                                                                           AS arr_per_user,
    arr_per_user
    / 12                                                                                                            AS monthly_price_per_user,
    DIV0(mart_arr.mrr, mart_arr.quantity)                                                                           AS mrr_check
  FROM mart_arr
  -- LEFT JOIN RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_ACCOUNT
  --     ON mart_arr.DIM_CRM_ACCOUNT_ID = MART_CRM_ACCOUNT.DIM_CRM_ACCOUNT_ID
  WHERE mart_arr.arr_month = '2023-01-01'
    AND mart_arr.quantity > 0
    AND mart_arr.product_tier_name LIKE '%Premium%'
    AND ((
      monthly_price_per_user >= 14
      AND monthly_price_per_user <= 16
    ) OR (monthly_price_per_user >= 7.5 AND monthly_price_per_user <= 9.5
    ))
    AND mart_arr.dim_crm_account_id IN
    (
      SELECT dim_crm_account_id
      --,product_rate_plan_name
      FROM mart_arr
      WHERE arr_month >= '2020-02-01'
        AND arr_month <= '2022-02-01'
        AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
    )
),

--Identifies accounts that had EoA pricing in the past few years
eoa_cohorts AS (
  SELECT DISTINCT dim_crm_account_id FROM
    eoa_cohorts_prep
),

--Identifies accounts that had their First Order discounted 70% as part of the Free User limits promotion
free_promo AS (
  SELECT DISTINCT dim_crm_account_id
  FROM mart_charge
  WHERE subscription_start_date >= '2023-02-01'
    AND rate_plan_charge_description = 'fo-discount-70-percent'
),

price_increase_prep AS (
  SELECT
    mart_charge.*,
    DIV0(mart_charge.arr, mart_charge.quantity) AS actual_price,
    prod.annual_billing_list_price              AS list_price
  FROM mart_charge
  INNER JOIN dim_product_detail AS prod
    ON mart_charge.dim_product_detail_id = prod.dim_product_detail_id
  WHERE mart_charge.subscription_start_date >= '2023-04-01'
    AND mart_charge.subscription_start_date <= '2023-07-01'
    AND mart_charge.type_of_arr_change = 'New'
    AND mart_charge.quantity > 0
    AND actual_price > 228
    AND actual_price < 290
    AND mart_charge.rate_plan_charge_name LIKE '%Premium%'
),

--Identifies accounts that received a First Order discount as part of the Premium price increase promotion
price_increase AS (
  SELECT DISTINCT dim_crm_account_id
  FROM price_increase_prep
),

--Identifies any accounts with Ultimate ARR
ultimate AS (
  SELECT DISTINCT
    dim_parent_crm_account_id,
    arr_month
  FROM mart_arr
  WHERE product_tier_name LIKE '%Ultimate%'
    AND arr > 0
),

--All AMER/APJ team accounts
amer_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('AMER SMB Sales', 'APAC SMB Sales')
    OR owner_role = 'Advocate_SMB_AMER'
),

--All EMEA team accounts
emea_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('EMEA SMB Sales')
    OR owner_role = 'Advocate_SMB_EMEA'
),

--Full set of account data, calculates Tier based on CARR/LAM Dev Count, identifies Team, and labels historical snapshots with the GDS_ACCOUNT_FLAG 
--if they would have been SMB at the time
account_base AS (
  SELECT
    acct.*,
    CASE
      WHEN first_high_value_case.case_id IS NOT NULL THEN 'Tier 1'
      WHEN first_high_value_case.case_id IS NULL
        THEN
          CASE
            WHEN acct.carr_this_account > 7000 THEN 'Tier 1'
            WHEN acct.carr_this_account < 3000 AND acct.parent_crm_account_lam_dev_count < 10 THEN 'Tier 3'
            ELSE 'Tier 2'
          END
    END                                                             AS calculated_tier,
    CASE
      WHEN (acct.snapshot_date >= '2024-02-01' AND amer_accounts.dim_crm_account_id IS NOT NULL)
        OR (
          acct.snapshot_date < '2024-02-01'
          AND (acct.parent_crm_account_geo IN ('AMER', 'APJ', 'APAC') OR acct.parent_crm_account_region IN ('AMER', 'APJ', 'APAC'))
        )
        THEN 'AMER/APJ'
      WHEN
        (acct.snapshot_date >= '2024-02-01' AND emea_accounts.dim_crm_account_id IS NOT NULL)
        OR (acct.snapshot_date < '2024-02-01' AND (acct.parent_crm_account_geo IN ('EMEA') OR acct.parent_crm_account_region IN ('EMEA')))
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
    first_order.fo_opp_name,
    churn.close_date                                                AS churn_close_date,
    churn.net_arr                                                   AS churn_net_arr,
    NOT COALESCE(fo_fiscal_year <= 2024, FALSE)                     AS new_fy25_fo_flag,
    first_high_value_case.created_date                              AS first_high_value_case_created_date,
    high_value_case.case_owner_name                                 AS high_value_case_owner,
    high_value_case.owner_id                                        AS high_value_case_owner_id,
    high_value_case.case_id                                         AS high_value_case_id,
    high_value_case.team                                            AS high_value_case_owner_team,
    high_value_case.manager_name                                    AS high_value_case_owner_manager,
    start_values.carr_account_family                                AS starting_carr_account_family,
    start_values.carr_this_account                                  AS starting_carr_this_account,
    CASE
      WHEN start_values.carr_this_account > 7000 THEN 'Tier 1'
      WHEN start_values.carr_this_account < 3000 AND start_values.parent_crm_account_lam_dev_count < 10 THEN 'Tier 3'
      ELSE 'Tier 2'
    END                                                             AS starting_calculated_tier,
    start_values.pte_score                                          AS starting_pte_score,
    start_values.ptc_score                                          AS starting_ptc_score,
    start_values.parent_crm_account_lam_dev_count                   AS starting_parent_crm_account_lam_dev_count,
    COALESCE(eoa_cohorts.dim_crm_account_id IS NOT NULL, FALSE)     AS eoa_flag,
    COALESCE(free_promo.dim_crm_account_id IS NOT NULL, FALSE)      AS free_promo_flag,
    COALESCE(price_increase.dim_crm_account_id IS NOT NULL, FALSE)  AS price_increase_promo_flag,
    COALESCE(ultimate.dim_parent_crm_account_id IS NOT NULL, FALSE) AS ultimate_customer_flag
  FROM dim_crm_account_daily_snapshot AS acct
  ------subquery that gets latest FO data
  LEFT JOIN first_order
    ON acct.dim_parent_crm_account_id = first_order.dim_parent_crm_account_id
  ---------subquery that gets latest churn data
  LEFT JOIN latest_churn AS churn
    ON acct.dim_crm_account_id = churn.dim_crm_account_id
  --------subquery to get high tier case owner
  LEFT JOIN high_value_case
    ON acct.dim_crm_account_id = high_value_case.account_id
  --------------subquery to get start of FY25 values
  LEFT JOIN start_values
    ON acct.dim_crm_account_id = start_values.dim_crm_account_id
  -----subquery to get FIRST high value case
  LEFT JOIN first_high_value_case
    ON acct.dim_crm_account_id = first_high_value_case.account_id
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
      AND ultimate.arr_month = DATE_TRUNC('month', acct.snapshot_date)
  ----- amer and apac accounts
  LEFT JOIN amer_accounts
    ON acct.dim_crm_account_id = amer_accounts.dim_crm_account_id
  ----- emea emea accounts
  LEFT JOIN emea_accounts
    ON acct.dim_crm_account_id = emea_accounts.dim_crm_account_id
  -------filtering to get current account data
  WHERE acct.snapshot_date = CURRENT_DATE
),

billing_accounts AS (
  SELECT DISTINCT
    dim_crm_account_id,
    po_required
  FROM dim_billing_account WHERE po_required = 'YES'
),

---------------need to add fields to create timeframes for case pulls 
--------------this table pulls in all of the data for the accounts (from account CTE), opportunities, subscription, 
-- and billing account (PO required field only) to act as the base table for the query 
account_blended AS (
  SELECT
    DATEADD(
      'day', 30, CURRENT_DATE
    )
      AS current_date_30_days,
    DATEADD(
      'day', 80, CURRENT_DATE
    )
      AS current_date_80_days,
    DATEADD(
      'day', 90, CURRENT_DATE
    )
      AS current_date_90_days,
    DATEADD(
      'day', 180, CURRENT_DATE
    )
      AS current_date_180_days,
    DATEADD(
      'day', 270, CURRENT_DATE
    )
      AS current_date_270_days,
    account_base.dim_crm_account_id
      AS account_id,
    account_base.gds_account_flag,
    account_base.account_owner,
    account_base.user_role_type,
    account_base.crm_account_owner_geo,
    account_base.parent_crm_account_sales_segment,
    account_base.parent_crm_account_business_unit,
    account_base.next_renewal_date,
    account_base.calculated_tier,
    account_base.team,
    account_base.high_value_case_id,
    account_base.high_value_case_owner,
    account_base.high_value_case_owner_id,
    account_base.high_value_case_owner_team,
    account_base.high_value_case_owner_manager,
    account_base.count_active_subscriptions,
    account_base.fo_opp_name,
    DATEDIFF(
      'day', CURRENT_DATE, account_base.next_renewal_date
    )
      AS days_till_next_renewal,
    -- DATEDIFF('day', last_upload_date, a.next_renewal_date) as days_from_last_upload_to_next_renewal,
    account_base.carr_account_family,
    account_base.carr_this_account,
    account_base.six_sense_account_buying_stage,
    account_base.six_sense_account_profile_fit,
    account_base.six_sense_account_intent_score,
    account_base.six_sense_account_update_date,
    account_base.gs_health_user_engagement,
    account_base.gs_health_cd,
    account_base.gs_health_devsecops,
    account_base.gs_health_ci,
    account_base.gs_health_scm,
    account_base.gs_first_value_date,
    account_base.gs_last_csm_activity_date,
    account_base.free_promo_flag,
    account_base.price_increase_promo_flag,
    DATEDIFF(
      'day', account_base.six_sense_account_update_date, CURRENT_DATE
    )
      AS days_since_6sense_account_update,
    -- DATEDIFF('day', SIX_SENSE_ACCOUNT_UPDATE_DATE, last_upload_date) as days_since_6sense_account_update_last_upload,
    mart_crm_opportunity.dim_crm_opportunity_id
      AS opportunity_id,
    mart_crm_opportunity.owner_id
      AS opportunity_owner_id,
    COALESCE(
      mart_crm_opportunity.subscription_type = 'Renewal'
      AND mart_crm_opportunity.close_date >= '2024-02-01'
      AND mart_crm_opportunity.close_date <= '2025-01-31',
      FALSE
    )                                                         AS fy25_renewal,
    mart_crm_opportunity.is_closed,
    mart_crm_opportunity.is_won,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.opportunity_term,
    mart_crm_opportunity.opportunity_name,
    mart_crm_opportunity.qsr_status,
    mart_crm_opportunity.qsr_notes,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.arr_basis,
    mart_crm_opportunity.ptc_predicted_renewal_risk_category,
    COALESCE(
      mart_crm_opportunity.deal_path_name = 'Partner',
      FALSE
    )                                                         AS partner_opp_flag,
    COALESCE(
      qsr_failed_last_75.qsr_failed_last_75 = TRUE,
      FALSE
    )                                                         AS qsr_failed_last_75,
    dim_subscription.dim_subscription_id
      AS sub_subscription_id,
    dim_subscription.dim_subscription_id_original
      AS sub_subscription_id_original,
    dim_subscription.subscription_status,
    dim_subscription.term_start_date
      AS current_subscription_start_date,
    dim_subscription.term_end_date
      AS current_subscription_end_date,
    DATEDIFF(
      'day', dim_subscription.term_start_date, CURRENT_DATE
    )
      AS days_into_current_subscription,
    -- DATEDIFF('day', s.term_start_date, last_upload_date) as days_into_current_subscription_last_upload,
    dim_subscription.turn_on_auto_renewal,
    last_case_data.created_date
      AS last_case_created_date,
    DATEDIFF(
      'day', CAST(last_case_data.created_date AS DATE), CURRENT_DATE
    )
      AS days_since_last_case_created,
    last_case_data.case_id
      AS last_case_id,
    last_case_data.subject
      AS last_case_subject,
    last_case_data.status
      AS last_case_status,
    last_case_data.dim_crm_user_id
      AS last_case_owner_id,
    DATEDIFF(
      'day', CAST(last_renewal_case_data.created_date AS DATE), CURRENT_DATE
    )
      AS days_since_last_renewal_case_created,
    last_renewal_case_data.case_id
      AS last_renewal_case_id,
    last_renewal_case_data.subject
      AS last_renewal_case_subject,
    last_renewal_case_data.status
      AS last_renewal_case_status,
    last_renewal_case_data.dim_crm_user_id
      AS last_renewal_case_owner_id,
    open_cases.count_cases
      AS current_open_cases,
    open_renewal_opps.count_open_opps,
    COUNT(DISTINCT dim_subscription.dim_subscription_id)
      OVER (PARTITION BY dim_subscription.dim_crm_account_id)
      AS count_subscriptions,
    COALESCE(
      high_adoption_last_90.ha_last_90 = TRUE,
      FALSE
    )                                                         AS ha_last_90,
    COALESCE(
      low_adoption_last_180.la_last_180 = TRUE,
      FALSE
    )                                                         AS la_last_180,
    COALESCE(
      high_value_case_last_90.high_value_last_90 = TRUE,
      FALSE
    )                                                         AS high_value_last_90,
    COALESCE(
      any_case_last_90.any_case_last_90 = TRUE,
      FALSE
    )                                                         AS any_case_last_90,
    dim_subscription.dim_subscription_id,
    CASE
      WHEN mart_crm_opportunity.subscription_type = 'Renewal' THEN DATEDIFF('day', CURRENT_DATE, mart_crm_opportunity.close_date)
    END
      AS days_till_close,
    COALESCE(
      mart_crm_opportunity.opportunity_name LIKE '%QSR%',
      FALSE
    )                                                         AS qsr_flag,
    COALESCE(
      qsr_flag = FALSE AND mart_crm_opportunity.subscription_type != 'Renewal',
      FALSE
    )                                                         AS non_qsr_non_renewal_oppty_flag,
    COALESCE(
      mart_crm_opportunity.subscription_type = 'Renewal',
      FALSE
    )                                                         AS renewal_flag,
    account_base.eoa_flag,
    billing_accounts.po_required,
    mart_crm_opportunity.auto_renewal_status,
    COALESCE(
      auto_renew_will_fail.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                         AS auto_renew_will_fail_mult_products,
    COALESCE(
      duo_on_sub.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                         AS duo_on_sub_flag,
    COALESCE((
      CONTAINS(mart_crm_opportunity.opportunity_name, '#ultimateupgrade')
      OR CONTAINS(mart_crm_opportunity.opportunity_name, 'Ultimate Upgrade')
      OR CONTAINS(mart_crm_opportunity.opportunity_name, 'Upgrade to Ultimate')
      OR CONTAINS(mart_crm_opportunity.opportunity_name, 'ultimate upgrade')
    ),
    FALSE)                                                    AS ultimate_upgrade_oppty_flag,
    COALESCE(
      tier1_full_churns.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                         AS tier1_full_churn_flag,
    COALESCE(
      existing_duo_trial.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                         AS existing_duo_trial_flag
  FROM account_base
  LEFT JOIN mart_crm_opportunity
    ON account_base.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
      AND DATEDIFF('day', CURRENT_DATE, mart_crm_opportunity.close_date) <= 365
  LEFT JOIN dim_subscription
    ON mart_crm_opportunity.dim_crm_opportunity_id = dim_subscription.dim_crm_opportunity_id_current_open_renewal
      AND dim_subscription.subscription_status = 'Active'
  LEFT JOIN billing_accounts
    ON account_base.dim_crm_account_id = billing_accounts.dim_crm_account_id
  LEFT JOIN last_case_data
    ON account_base.dim_crm_account_id = last_case_data.dim_crm_account_id
  LEFT JOIN last_renewal_case_data
    ON account_base.dim_crm_account_id = last_renewal_case_data.dim_crm_account_id
  LEFT JOIN open_cases
    ON account_base.dim_crm_account_id = open_cases.dim_crm_account_id
  LEFT JOIN open_renewal_opps
    ON account_base.dim_crm_account_id = open_renewal_opps.dim_crm_account_id
  LEFT JOIN low_adoption_last_180
    ON account_base.dim_crm_account_id = low_adoption_last_180.dim_crm_account_id
  LEFT JOIN high_adoption_last_90
    ON account_base.dim_crm_account_id = high_adoption_last_90.dim_crm_account_id
  LEFT JOIN qsr_failed_last_75
    ON account_base.dim_crm_account_id = qsr_failed_last_75.dim_crm_account_id
      AND mart_crm_opportunity.dim_crm_opportunity_id = qsr_failed_last_75.dim_crm_opportunity_id
  LEFT JOIN high_value_case_last_90
    ON account_base.dim_crm_account_id = high_value_case_last_90.dim_crm_account_id
  LEFT JOIN any_case_last_90
    ON account_base.dim_crm_account_id = any_case_last_90.dim_crm_account_id
  LEFT JOIN auto_renew_will_fail
    ON account_base.dim_crm_account_id = auto_renew_will_fail.dim_crm_account_id
      AND dim_subscription.dim_subscription_id = auto_renew_will_fail.dim_subscription_id
  LEFT JOIN tier1_full_churns
    ON account_base.dim_crm_account_id = tier1_full_churns.dim_crm_account_id
  LEFT JOIN existing_duo_trial
    ON account_base.dim_crm_account_id = existing_duo_trial.dim_crm_account_id
  LEFT JOIN duo_on_sub
    ON account_base.dim_crm_account_id = duo_on_sub.dim_crm_account_id
      AND dim_subscription.dim_subscription_id = duo_on_sub.dim_subscription_id
  WHERE (account_base.gds_account_flag = TRUE AND mart_crm_opportunity.subscription_type != 'Renewal')
    OR (
      (
        account_base.gds_account_flag = TRUE
        AND mart_crm_opportunity.subscription_type = 'Renewal'
        AND LOWER(mart_crm_opportunity.opportunity_name) NOT LIKE '%edu program%'
      )
      OR (
        account_base.gds_account_flag = TRUE
        AND mart_crm_opportunity.subscription_type = 'Renewal'
        AND LOWER(mart_crm_opportunity.opportunity_name) NOT LIKE '%oss program%'
      )
    )
),

--Current month basic user count information
monthly_mart AS (
  SELECT DISTINCT
    snapshot_month,
    dim_subscription_id_original,
    instance_type,
    subscription_status,
    subscription_start_date,
    subscription_end_date,
    term_end_date,
    license_user_count,
    MAX(billable_user_count) OVER (PARTITION BY dim_subscription_id_original) AS max_billable_user_count
  FROM mart_product_usage_paid_user_metrics_monthly
  WHERE instance_type = 'Production'
    AND subscription_status = 'Active'
    AND snapshot_month = DATE_TRUNC('month', CURRENT_DATE)
),

--Calculates utilization at the user/account level, pricing, quantity
utilization AS (
  SELECT
    mart_arr.arr_month,
    monthly_mart.snapshot_month,
    mart_arr.subscription_end_month,
    mart_arr.dim_crm_account_id,
    mart_arr.crm_account_name,
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
    COALESCE(mart_arr.contract_seat_reconciliation = 'Yes' AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE)  AS qsr_enabled_flag,
    mart_arr.product_tier_name,
    mart_arr.product_delivery_type,
    mart_arr.product_rate_plan_name,
    mart_arr.arr,
    DIV0(mart_arr.arr, mart_arr.quantity)                                                                            AS arr_per_user,
    monthly_mart.max_billable_user_count,
    monthly_mart.license_user_count,
    monthly_mart.subscription_start_date,
    monthly_mart.subscription_end_date,
    monthly_mart.term_end_date,
    monthly_mart.max_billable_user_count - monthly_mart.license_user_count                                           AS overage_count,
    DIV0(mart_arr.arr, mart_arr.quantity) * (monthly_mart.max_billable_user_count - monthly_mart.license_user_count) AS overage_amount,
    MAX(monthly_mart.snapshot_month) OVER (PARTITION BY monthly_mart.dim_subscription_id_original)                   AS latest_overage_month
  FROM mart_arr
  LEFT JOIN monthly_mart
    ON mart_arr.dim_subscription_id_original = monthly_mart.dim_subscription_id_original
  WHERE mart_arr.arr_month = monthly_mart.snapshot_month
    AND mart_arr.product_tier_name NOT LIKE '%Storage%'
    AND mart_arr.quantity > 0
),

--Identifies accounts where a user has manually switched off autorenew (canceled subscription)
auto_renew_switch_one AS (
  SELECT
    dim_date.date_actual
      AS snapshot_date,
    prep_billing_account_user.user_name
      AS update_user,
    prep_billing_account_user.is_integration_user,
    dim_subscription_snapshot_bottom_up.turn_on_auto_renewal
      AS active_autorenew_status,
    LAG(dim_subscription_snapshot_bottom_up.turn_on_auto_renewal, 1)
      OVER (PARTITION BY dim_subscription_snapshot_bottom_up.subscription_name ORDER BY dim_date.date_actual ASC)
      AS prior_auto_renewal,
    LEAD(dim_subscription_snapshot_bottom_up.turn_on_auto_renewal, 1)
      OVER (PARTITION BY dim_subscription_snapshot_bottom_up.subscription_name ORDER BY dim_date.date_actual ASC)
      AS future_auto_renewal,
    dim_subscription_snapshot_bottom_up.*
  FROM dim_subscription_snapshot_bottom_up
  INNER JOIN dim_date ON dim_subscription_snapshot_bottom_up.snapshot_id = dim_date.date_id
  INNER JOIN prep_billing_account_user ON dim_subscription_snapshot_bottom_up.updated_by_id = prep_billing_account_user.zuora_user_id
  WHERE snapshot_date >= '2023-02-01'
    AND snapshot_date < CURRENT_DATE
    AND dim_subscription_snapshot_bottom_up.subscription_status = 'Active'
  ORDER BY 1 ASC
),


----------------------------get the date on a subscription when a customer switches from auto-renew on to auto-renew off
autorenew_switch AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    active_autorenew_status,
    prior_auto_renewal,
    MAX(snapshot_date) AS latest_switch_date
  FROM auto_renew_switch_one
  WHERE
    ((active_autorenew_status = 'No' AND update_user = 'svc_zuora_fulfillment_int@gitlab.com') AND prior_auto_renewal = 'Yes')
  GROUP BY 1, 2, 3, 4
),

bronze_starter_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_arr
  WHERE arr_month >= '2020-02-01'
    AND arr_month <= '2022-02-01'
    AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
),

--Duplicative - identifies EOA accounts
eoa_accounts_fy24_one AS (
  SELECT
    mart_arr.arr_month,
    mart_arr.subscription_end_month,
    mart_arr.dim_crm_account_id,
    mart_arr.crm_account_name,
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
    DIV0(mart_arr.arr, mart_arr.quantity)                                                                           AS arr_per_user,
    arr_per_user
    / 12                                                                                                            AS monthly_price_per_user,
    DIV0(mart_arr.mrr, mart_arr.quantity)                                                                           AS mrr_check
  FROM mart_arr
  LEFT JOIN bronze_starter_accounts
    ON mart_arr.dim_crm_account_id = bronze_starter_accounts.dim_crm_account_id
  WHERE mart_arr.arr_month = '2023-01-01'
    AND mart_arr.product_tier_name LIKE '%Premium%'
    AND ((
      monthly_price_per_user >= 14
      AND mart_arr.quantity > 0
      AND monthly_price_per_user <= 16
    ) OR (monthly_price_per_user >= 7.5 AND monthly_price_per_user <= 9.5
    ))
    AND bronze_starter_accounts.dim_crm_account_id IS NOT NULL
),

--Looks for current month ARR around $15 to account for currency conversion.
--Checks to make sure accounts previously had ARR in Bronze or Starter (to exclude accounts that just have discounts)
eoa_accounts_fy24 AS (
  SELECT DISTINCT dim_crm_account_id
  FROM eoa_accounts_fy24_one
),

prior_sub_25_eoa_prep AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal,
    DIV0(mart_charge.previous_mrr, mart_charge.previous_quantity) AS previous_price,
    DIV0(mart_charge.mrr, mart_charge.quantity)                   AS price_after_renewal
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  INNER JOIN eoa_accounts_fy24 ON mart_charge.dim_crm_account_id = eoa_accounts_fy24.dim_crm_account_id
  -- left join PROD.RESTRICTED_SAFE_COMMON_MART_SALES.mart_crm_opportunity on 
  -- dim_subscription.dim_crm_opportunity_id_current_open_renewal = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE mart_charge.rate_plan_name LIKE '%Premium%'
    AND mart_charge.type_of_arr_change != 'New'
    AND mart_charge.term_start_date >= '2023-02-01'
    AND mart_charge.term_start_date < CURRENT_DATE
    AND price_after_renewal > previous_price
    AND mart_charge.previous_quantity != 0
    AND mart_charge.quantity != 0
    AND (
      LOWER(mart_charge.rate_plan_charge_description) LIKE '%eoa%'
      OR (
        (previous_price >= 5 AND previous_price <= 7)
        OR (previous_price >= 8 AND previous_price <= 10)
        OR (previous_price >= 14 AND previous_price <= 16)
      )
    )
    AND mart_charge.quantity < 25
),

--Accounts that go from <25 users to >25 users have a pricing structure change if they are still under an EoA discounted ramp
prior_sub_25_eoa AS (
  SELECT DISTINCT dim_crm_account_id
  FROM
    prior_sub_25_eoa_prep
),

duo_trials_one_prep AS (
  SELECT DISTINCT
    contact_id,
    account_id,
    marketo_last_interesting_moment_date
  FROM sfdc_contact_snapshots_source
  WHERE marketo_last_interesting_moment LIKE '%Duo Pro SaaS Trial%'
    AND marketo_last_interesting_moment_date >= '2024-02-01'
),

--Identifies accounts/contacts with Duo Trials
duo_trials_one AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY marketo_last_interesting_moment_date) AS order_number
  FROM duo_trials_one_prep
),

duo_trials AS (
  SELECT *
  FROM duo_trials_one
  WHERE order_number = 1
),

-- payment_failure_tasks AS (
--   SELECT DISTINCT
--     dim_crm_account_id,
--     task_id,
--     task_date,
--       --dim_crm_opportunity_id, 
--     RIGHT(REGEXP_SUBSTR(full_comments, 'GitLab subscription.{12}'), 11) AS extracted_subscription_name
--   FROM mart_crm_task
--   WHERE (task_subject LIKE '%Payment Failed%' OR task_subject LIKE '%Payment failed%')
--     --AND task_id = '00TPL000006KMiV2AW'
--     AND task_date >= '2024-02-01'
-- ),


--Identifies subscriptions with payment failures
failure_sub AS (
  SELECT
    dim_subscription.dim_subscription_id                                                          AS failure_subscription_id,
    dim_subscription.dim_crm_opportunity_id                                                       AS failed_sub_oppty,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal                                  AS failed_sub_renewal_opp,
    dim_subscription.dim_crm_opportunity_id_closed_lost_renewal                                   AS failed_sub_closed_opp,
    dim_subscription.dim_crm_account_id,
    CAST(RIGHT(REGEXP_SUBSTR(zuora_subscription_source.notes, 'RENEWAL_ERROR.{12}'), 10) AS DATE) AS failure_date
  FROM dim_subscription
  LEFT JOIN zuora_subscription_source
    ON dim_subscription.dim_subscription_id = zuora_subscription_source.subscription_id
  WHERE LOWER(zuora_subscription_source.notes) LIKE '%renewal_error%'
    AND REGEXP_SUBSTR(zuora_subscription_source.notes, 'RENEWAL_ERROR.{12}') LIKE '%(20%'
),

-----------brings together account blended date, utilization data, and autorenewal switch data 
all_data AS (
  SELECT
    account_blended.*,
    CASE WHEN utilization.subscription_end_month > '2024-04-01' THEN 29 ELSE 23.78 END AS future_price,
    utilization.overage_count,
    utilization.overage_amount,
    utilization.latest_overage_month,
    utilization.qsr_enabled_flag,
    utilization.max_billable_user_count,
    utilization.license_user_count,
    utilization.arr,
    utilization.arr_per_user,
    utilization.turn_on_seat_reconciliation,
    --turn_on_auto_renewal,
    autorenew_switch.latest_switch_date,
    DATEDIFF('day', autorenew_switch.latest_switch_date, CURRENT_DATE)                 AS days_since_autorenewal_switch,
    COALESCE(prior_sub_25_eoa.dim_crm_account_id IS NOT NULL, FALSE)                   AS prior_year_eoa_under_25_flag,
    COALESCE(duo_trials.contact_id IS NOT NULL, FALSE)                                 AS duo_trial_on_account_flag,
    duo_trials.marketo_last_interesting_moment_date                                    AS duo_trial_start_date,
    duo_trials.contact_id,
    duo_trials.order_number,
    COALESCE(failure_sub.dim_crm_account_id IS NOT NULL, FALSE)                        AS payment_failure_flag,
    failure_sub.failed_sub_oppty,
    failure_sub.failed_sub_renewal_opp,
    failure_sub.failed_sub_closed_opp,
    failure_sub.failure_date
  FROM account_blended
  LEFT JOIN utilization
    ON utilization.dim_crm_account_id = account_blended.account_id
    --and term_end_date = current_subscription_end_date
      AND account_blended.sub_subscription_id_original = utilization.dim_subscription_id_original
  LEFT JOIN autorenew_switch
    ON account_blended.account_id = autorenew_switch.dim_crm_account_id
      AND account_blended.sub_subscription_id = autorenew_switch.dim_subscription_id
  LEFT JOIN prior_sub_25_eoa
    ON account_blended.account_id = prior_sub_25_eoa.dim_crm_account_id
  LEFT JOIN duo_trials
    ON account_blended.account_id = duo_trials.account_id
  LEFT JOIN failure_sub
    ON account_blended.account_id = failure_sub.dim_crm_account_id
      AND account_blended.sub_subscription_id = failure_sub.failure_subscription_id
),

------------flags each account/opportunity with any applicable flags
case_flags AS (
  SELECT
    *,
    COALESCE(
      calculated_tier = 'Tier 1' AND high_value_last_90 = FALSE AND tier1_full_churn_flag = FALSE,
      FALSE
    ) AS check_in_case_needed_flag,
    -- COALESCE(
    --   calculated_tier IN ('Tier 2', 'Tier 3') AND any_case_last_90 = FALSE AND (future_price * max_billable_user_count) * 12 >= 7000,
    --   FALSE
    -- ) AS future_tier_1_account_flag,
    COALESCE(
      subscription_type = 'Renewal' AND po_required = 'YES',
      FALSE
    ) AS po_required_flag,
    COALESCE(
      opportunity_term > 12
      OR (CONTAINS(LOWER(opportunity_name), '2 year') OR CONTAINS(LOWER(opportunity_name), '3 year') OR CONTAINS(LOWER(opportunity_name), 'ramp')),
      FALSE
    ) AS multiyear_renewal_flag,
    COALESCE(
      (LOWER(auto_renewal_status) LIKE '%eoa%'),
      FALSE
    ) AS eoa_auto_renewal_will_fail_flag,
    COALESCE(
      ((auto_renewal_status IS NOT NULL OR auto_renewal_status NOT IN ('On', 'Off')) AND eoa_auto_renewal_will_fail_flag = FALSE),
      FALSE
    ) AS auto_renewal_will_fail_flag,
    COALESCE(
      auto_renew_will_fail_mult_products = TRUE,
      FALSE
    ) AS auto_renewal_will_fail_logic_flag,
    COALESCE(
      (eoa_flag = TRUE AND prior_year_eoa_under_25_flag = TRUE AND max_billable_user_count >= 25) OR (eoa_flag = TRUE AND turn_on_auto_renewal = 'No'),
      FALSE
    ) AS eoa_renewal_flag,
    COALESCE(
      qsr_flag = TRUE AND qsr_status = 'Failed' AND qsr_notes LIKE '%card%' AND is_closed = FALSE AND close_date >= '2024-02-01',
      FALSE
    ) AS qsr_failed_flag,
    COALESCE(
      calculated_tier IN ('Tier 2') AND subscription_type = 'Renewal' AND ptc_predicted_renewal_risk_category = 'Will Churn (Actionable)',
      FALSE
    ) AS will_churn_flag,
    COALESCE(
      latest_switch_date IS NOT NULL,
      FALSE
    ) AS auto_renew_recently_turned_off_flag,
    --This is old logic and should have been removed
    -- COALESCE(
    --   calculated_tier IN ('Tier 2', 'Tier 3')
    --   AND overage_count > 0
    --   AND overage_amount > 0
    --   AND latest_overage_month = DATE_TRUNC('month', CURRENT_DATE)
    --   AND qsr_enabled_flag = FALSE,
    --   FALSE
    -- ) AS overage_qsr_off_flag,
    COALESCE(
      duo_trial_on_account_flag = TRUE,
      FALSE
    ) AS duo_trial_flag,
    COALESCE(
      duo_on_sub_flag = TRUE AND subscription_type = 'Renewal',
      FALSE
    ) AS duo_renewal_flag,
    COALESCE(
      calculated_tier IN ('Tier 2') AND payment_failure_flag = TRUE AND subscription_type = 'Renewal',
      FALSE
    ) AS renewal_payment_failure,
    COALESCE(
      overage_amount > 0
      AND turn_on_seat_reconciliation != 'Yes'
      AND latest_overage_month = DATE_TRUNC('month', CURRENT_DATE)
      AND CONTAINS(LOWER(fo_opp_name), '#smb19promo'),
      FALSE
    ) AS overage_with_qsr_off_19_offer,
    COALESCE(
      overage_amount > 1000
      AND turn_on_seat_reconciliation != 'Yes'
      AND latest_overage_month = DATE_TRUNC('month', CURRENT_DATE)
      AND overage_with_qsr_off_19_offer = FALSE,
      FALSE
    ) AS overage_with_qsr_off
  FROM all_data
  WHERE is_closed = FALSE
),

-------uses the case flags to create cases and adds in the timing they should be triggering 
cases AS (
  SELECT
    *,
    CASE
      WHEN
        check_in_case_needed_flag = TRUE
        AND (
          (current_subscription_end_date = current_date_80_days)
          OR (current_subscription_end_date = current_date_180_days)
          OR (current_subscription_end_date = current_date_270_days)
        )
        THEN 'High Value Account Check In'
    END AS check_in_trigger_name,
    CASE WHEN duo_renewal_flag = TRUE AND (current_subscription_end_date = current_date_30_days) THEN 'Renewal with Duo'
    -- WHEN
    --   future_tier_1_account_flag = TRUE AND (current_subscription_end_date = current_date_90_days) AND any_case_last_90 = FALSE
    -- THEN 'Future Tier 1 Account Check In'
    END AS future_tier_1_trigger_name,
    CASE WHEN po_required_flag = TRUE AND (current_subscription_end_date = current_date_90_days) AND arr_basis > 3000 THEN 'PO Required'
      WHEN multiyear_renewal_flag = TRUE AND (current_subscription_end_date = current_date_90_days) AND arr_basis > 3000 THEN 'Multiyear Renewal'
      WHEN eoa_auto_renewal_will_fail_flag = TRUE AND (current_subscription_end_date = current_date_90_days) THEN 'Auto-Renewal Will Fail'
      WHEN auto_renewal_will_fail_flag = TRUE AND (current_subscription_end_date = current_date_90_days) AND arr_basis > 3000 THEN 'Auto-Renewal Will Fail'
      WHEN auto_renewal_will_fail_logic_flag = TRUE AND (current_subscription_end_date = current_date_90_days) THEN 'Auto-Renewal Will Fail'
      WHEN eoa_renewal_flag = TRUE AND (current_subscription_end_date = current_date_90_days) THEN 'EOA Renewal'
      WHEN will_churn_flag = TRUE AND (current_subscription_end_date = current_date_90_days) THEN 'Renewal Risk: Will Churn'
      WHEN renewal_payment_failure = TRUE AND failure_date = DATEADD('day', -1, CURRENT_DATE) THEN 'Renewal with Payment Failure'
      WHEN
        auto_renew_recently_turned_off_flag = TRUE AND latest_switch_date = DATEADD('day', -1, CURRENT_DATE) AND (eoa_flag = FALSE AND arr_basis > 3000)
        THEN 'Auto-Renew Recently Turned Off'
      WHEN
        auto_renew_recently_turned_off_flag = TRUE AND latest_switch_date = DATEADD('day', -1, CURRENT_DATE) AND eoa_flag = TRUE
        THEN 'EOA Account - Auto-Renew Recently Turned Off'
      WHEN qsr_failed_flag = TRUE AND qsr_failed_last_75 = FALSE AND net_arr > 1000 THEN 'Failed QSR'
    END
      AS renewal_trigger_name,
    CASE WHEN
        overage_with_qsr_off_19_offer = TRUE
        AND (
          (current_subscription_end_date = current_date_80_days)
          OR (current_subscription_end_date = current_date_180_days)
          OR (current_subscription_end_date = current_date_270_days)
        )
        THEN 'Overage and QSR Off'
      WHEN overage_with_qsr_off AND ha_last_90 = FALSE THEN 'Overage and QSR Off'
      WHEN duo_trial_flag = TRUE AND existing_duo_trial_flag = FALSE AND duo_trial_start_date > DATEADD('day', -1, CURRENT_DATE) THEN 'Duo Trial Started'
    END
      AS high_adoption_case_trigger_name
  FROM case_flags
),

--------------prioritizes renewals first and then low adoption over high adoption
final AS (
  SELECT
    *,
    CASE WHEN check_in_trigger_name IS NOT NULL THEN check_in_trigger_name
      WHEN future_tier_1_trigger_name IS NOT NULL THEN future_tier_1_trigger_name
      WHEN renewal_trigger_name IS NOT NULL THEN renewal_trigger_name
      WHEN (renewal_trigger_name IS NULL AND high_adoption_case_trigger_name IS NOT NULL) THEN high_adoption_case_trigger_name
      --WHEN (renewal_trigger_name IS NULL and low_adoption_case_trigger_name IS NULL and high_adoption_case_trigger_name IS NOT NULL) 
      -- then high_adoption_case_trigger_name
    END                                               AS case_trigger,
    COALESCE(renewal_trigger_name IS NOT NULL, FALSE) AS renewal_case
  FROM cases
  WHERE case_trigger IS NOT NULL
),

distinct_cases AS (
  SELECT DISTINCT
    final.case_trigger,
    final.account_id,
    final.calculated_tier
      AS account_tier,
    COALESCE(ROUND(final.arr_per_user, 2), 0)
      AS current_price,
    COALESCE(final.future_price * 12, 0)
      AS renewal_price,
    case_data.case_trigger_id,
    case_data.status,
    case_data.case_origin,
    case_data.type,
    CASE WHEN case_data.case_trigger_id IN (6, 26) THEN COALESCE(CONCAT(case_data.case_subject, ' ', final.latest_switch_date), case_data.case_subject)
      WHEN case_data.case_trigger_id IN (28) THEN COALESCE(CONCAT(case_data.case_subject, ' ', final.duo_trial_start_date), case_data.case_subject)
      WHEN case_data.case_trigger_id IN (31, 32) THEN CONCAT(case_data.case_subject, ' ', final.failure_date)
      WHEN renewal_case = TRUE AND case_data.case_trigger_id NOT IN (31, 32) THEN CONCAT(case_data.case_subject, ' ', final.close_date) ELSE case_data.case_subject
    END                                       AS case_subject,
    CASE WHEN final.calculated_tier = 'Tier 1' THEN final.high_value_case_owner_id
      WHEN final.current_open_cases > 0 THEN final.last_case_owner_id
      ELSE case_data.owner_id
    END
      AS owner_id,
    case_data.case_reason,
    case_data.record_type_id,
    case_data.priority,
    CASE
      WHEN case_data.case_trigger_id IN (10, 11, 12, 28) OR final.case_trigger = 'Duo Trial Started' THEN NULL ELSE final.opportunity_id
    END                                       AS case_opportunity_id,
    CASE
      WHEN case_data.case_trigger_id IN (28) THEN final.contact_id
    END                                       AS case_contact_id,
    case_data.case_cta,
    CASE
      -- WHEN case_data.case_trigger_id = 9 then CONCAT(case_data.CASE_CONTEXT, ' ', ptc_insights, ' EOA Account:', eoa_flag)
      -- WHEN case_data.case_trigger_id = 11 then CONCAT(case_data.CASE_CONTEXT, ' ', pte_insights, ' EOA Account:', eoa_flag)
      WHEN
        case_data.case_trigger_id IN (6, 26)
        THEN CONCAT(
            case_data.case_context,
            ' ',
            final.latest_switch_date,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id IN (5, 25)
        THEN CONCAT(
            case_data.case_context,
            ' ',
            final.current_subscription_end_date,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id = 7
        THEN CONCAT(
            case_data.case_context,
            ' ',
            final.opportunity_id,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id = 4 AND final.auto_renewal_will_fail_logic_flag = FALSE
        THEN CONCAT(
            case_data.case_context,
            ' ',
            COALESCE(final.auto_renewal_status, 'Null'),
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id = 4 AND final.auto_renewal_will_fail_logic_flag = TRUE
        THEN CONCAT(
            'Auto Renewal Will Fail Due to Multiple Products on Subscription ',
            case_data.case_context,
            ' ',
            COALESCE(final.auto_renewal_status, 'Null'),
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id = 12
        THEN CONCAT(
            case_data.case_context,
            ' ',
            final.six_sense_account_intent_score,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      WHEN
        case_data.case_trigger_id = 16
        THEN CONCAT(
            'Current Billable Quantity: ',
            final.max_billable_user_count,
            ' Current Price: ',
            ROUND(final.arr_per_user, 2),
            ' Renewal Price: ',
            final.future_price * 12,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag
          )
      WHEN
        case_data.case_trigger_id = 10
        THEN CONCAT(
            case_data.case_context,
            '$19 Promo:',
            final.overage_with_qsr_off_19_offer,
            ' EOA Account:',
            final.eoa_flag,
            ' Partner Opp:',
            final.partner_opp_flag,
            ' Free Promo Flag:',
            final.free_promo_flag,
            ' Price Increase Flag:',
            final.price_increase_promo_flag,
            ' Current Price: ',
            COALESCE(ROUND(final.arr_per_user, 2), 0),
            ' Renewal Price: ',
            COALESCE(final.future_price * 12, 0)
          )
      ELSE CONCAT(
          case_data.case_context,
          ' EOA Account:',
          final.eoa_flag,
          ' Partner Opp:',
          final.partner_opp_flag,
          ' Free Promo Flag:',
          final.free_promo_flag,
          ' Price Increase Flag:',
          final.price_increase_promo_flag,
          ' Current Price: ',
          COALESCE(ROUND(final.arr_per_user, 2), 0),
          ' Renewal Price: ',
          COALESCE(final.future_price * 12, 0)
        )
    END                                       AS context
  FROM final
  LEFT JOIN case_data
    ON final.case_trigger = case_data.case_trigger
),

case_output AS (
  SELECT
    distinct_cases.* EXCLUDE (context),
    CASE WHEN distinct_cases.owner_id != '00G8X000006WmU3' THEN CONCAT(distinct_cases.context, ' Skip Queue Flag: True')
      ELSE CONCAT(distinct_cases.context, ' Skip Queue Flag: False')
    END                                                                                                              AS context_final,
    COALESCE(distinct_cases.owner_id IS NULL AND distinct_cases.case_trigger = 'High Value Account Check In', FALSE) AS remove_flag,
    CURRENT_DATE                                                                                                     AS query_run_date
  FROM distinct_cases
  LEFT JOIN prep_crm_case
    ON distinct_cases.account_id = prep_crm_case.dim_crm_account_id
      AND distinct_cases.case_subject = prep_crm_case.subject
      AND (
        distinct_cases.case_opportunity_id = prep_crm_case.dim_crm_opportunity_id OR (distinct_cases.case_opportunity_id IS NULL AND prep_crm_case.created_date >= DATEADD('day', -30, CURRENT_DATE))
        OR
        (prep_crm_case.dim_crm_opportunity_id IS NULL AND prep_crm_case.created_date >= DATEADD('day', -30, CURRENT_DATE))
      )

  WHERE remove_flag = FALSE
    AND prep_crm_case.dim_crm_account_id IS NULL
)

{{ dbt_audit(
    cte_ref="case_output",
    created_by="@sglad",
    updated_by="@mfleisher",
    created_date="2024-07-02",
    updated_date="2024-07-30"
) }}
