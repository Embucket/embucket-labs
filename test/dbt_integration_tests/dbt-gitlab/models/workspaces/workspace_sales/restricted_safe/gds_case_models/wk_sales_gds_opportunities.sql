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
    ('dim_namespace', 'dim_namespace'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('fct_trial_latest', 'fct_trial_latest'),
    ('bdg_namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_order', 'dim_order'),
    ('dim_order_action', 'dim_order_action'),
    ('mart_delta_arr_subscription_month', 'mart_delta_arr_subscription_month'),
    ('case_data', 'wk_sales_gds_cases'),
    ('wk_sales_gds_fo_buckets', 'wk_sales_gds_fo_buckets'),
    ('case_history', 'prep_crm_case_history'),
    ('wk_sales_gds_account_snapshots', 'wk_sales_gds_account_snapshots')
    ])

}},



opportunity_data AS (
  SELECT
    mart_crm_opportunity.* exclude (model_created_date,model_updated_date, created_by, updated_by,dbt_updated_at,dbt_created_at),
    user.user_role_type,
    dim_date.fiscal_year                                                              AS date_range_year,
    dim_date.fiscal_quarter_name_fy                                                   AS date_range_quarter,
    DATE_TRUNC(MONTH, dim_date.date_actual)                                           AS date_range_month,
    dim_date.first_day_of_week                                                        AS date_range_week,
    dim_date.date_id                                                                  AS date_range_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    dim_date.first_day_of_fiscal_quarter,
    CASE
      when mart_crm_opportunity.product_category LIKE '%Self%' OR mart_crm_opportunity.product_details LIKE '%Self%' OR mart_crm_opportunity.product_category LIKE '%Starter%'
        OR mart_crm_opportunity.product_details LIKE '%Starter%' THEN 'Self-Managed'
      when mart_crm_opportunity.product_category LIKE '%SaaS%' OR mart_crm_opportunity.product_details LIKE '%SaaS%' OR mart_crm_opportunity.product_category LIKE '%Bronze%'
        OR mart_crm_opportunity.product_details LIKE '%Bronze%' OR mart_crm_opportunity.product_category LIKE '%Silver%' OR mart_crm_opportunity.product_details LIKE '%Silver%'
        OR mart_crm_opportunity.product_category LIKE '%Gold%' OR mart_crm_opportunity.product_details LIKE '%Gold%' THEN 'SaaS'
      when mart_crm_opportunity.product_details NOT LIKE '%SaaS%'
        AND (mart_crm_opportunity.product_details LIKE '%Premium%' OR mart_crm_opportunity.product_details LIKE '%Ultimate%') THEN 'Self-Managed'
      when mart_crm_opportunity.product_category LIKE '%Storage%' OR mart_crm_opportunity.product_details LIKE '%Storage%' THEN 'Storage'
      ELSE 'Other'
    END                                                                               AS delivery,
    CASE
      WHEN mart_crm_opportunity.order_type LIKE '3%' OR mart_crm_opportunity.order_type LIKE '2%' THEN 'Growth'
      WHEN mart_crm_opportunity.order_type LIKE '1%' THEN 'First Order'
      WHEN mart_crm_opportunity.order_type LIKE '4%' OR mart_crm_opportunity.order_type LIKE '5%' OR mart_crm_opportunity.order_type LIKE '6%' THEN 'Churn / Contraction'
    END                                                                               AS order_type_clean,
    COALESCE (mart_crm_opportunity.order_type LIKE '5%' AND mart_crm_opportunity.net_arr = 0, FALSE)                            AS partial_churn_0_narr_flag,
    CASE
      when mart_crm_opportunity.product_category LIKE '%Premium%' OR mart_crm_opportunity.product_details LIKE '%Premium%' THEN 'Premium'
      when mart_crm_opportunity.product_category LIKE '%Ultimate%' OR mart_crm_opportunity.product_details LIKE '%Ultimate%' THEN 'Ultimate'
      when mart_crm_opportunity.product_category LIKE '%Bronze%' OR mart_crm_opportunity.product_details LIKE '%Bronze%' THEN 'Bronze'
      when mart_crm_opportunity.product_category LIKE '%Starter%' OR mart_crm_opportunity.product_details LIKE '%Starter%' THEN 'Starter'
      when mart_crm_opportunity.product_category LIKE '%Storage%' OR mart_crm_opportunity.product_details LIKE '%Storage%' THEN 'Storage'
      when mart_crm_opportunity.product_category LIKE '%Silver%' OR mart_crm_opportunity.product_details LIKE '%Silver%' THEN 'Silver'
      when mart_crm_opportunity.product_category LIKE '%Gold%' OR mart_crm_opportunity.product_details LIKE '%Gold%' THEN 'Gold'
      when mart_crm_opportunity.product_category LIKE 'CI%' OR mart_crm_opportunity.product_details LIKE 'CI%' THEN 'CI'
      when mart_crm_opportunity.product_category LIKE '%omput%' OR mart_crm_opportunity.product_details LIKE '%omput%' THEN 'CI'
      when mart_crm_opportunity.product_category LIKE '%Duo%' OR mart_crm_opportunity.product_details LIKE '%Duo%' THEN 'Duo Pro'
      when mart_crm_opportunity.product_category LIKE '%uggestion%' OR mart_crm_opportunity.product_details LIKE '%uggestion%' THEN 'Duo Pro'
      when mart_crm_opportunity.product_category LIKE '%Agile%' OR mart_crm_opportunity.product_details LIKE '%Agile%' THEN 'Enterprise Agile Planning'
      ELSE mart_crm_opportunity.product_category
    END                                                                               AS product_tier,
    COALESCE (LOWER(mart_crm_opportunity.product_details) LIKE ANY ('%code suggestions%', '%duo%'), FALSE) AS duo_flag,
    COALESCE (mart_crm_opportunity.opportunity_name LIKE '%QSR%', FALSE)                                   AS qsr_flag,
    CASE
      WHEN mart_crm_opportunity.order_type LIKE '7%' AND qsr_flag = FALSE THEN 'PS/CI/CD'
      WHEN mart_crm_opportunity.order_type LIKE '1%' AND mart_crm_opportunity.net_arr > 0 THEN 'First Order'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr > 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        AND qsr_flag = FALSE THEN 'Growth - Uplift'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND mart_crm_opportunity.net_arr > 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Uplift'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND mart_crm_opportunity.net_arr = 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Flat'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND mart_crm_opportunity.net_arr < 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Contraction'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr > 0 AND mart_crm_opportunity.subscription_type = 'Renewal'
        THEN 'Renewal - Uplift'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr < 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        THEN 'Non-Renewal - Contraction'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr = 0 AND mart_crm_opportunity.subscription_type != 'Renewal'
        THEN 'Non-Renewal - Flat'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr = 0 AND mart_crm_opportunity.subscription_type = 'Renewal'
        THEN 'Renewal - Flat'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('2.%', '3.%', '4.%') AND mart_crm_opportunity.net_arr < 0 AND mart_crm_opportunity.subscription_type = 'Renewal'
        THEN 'Renewal - Contraction'
      WHEN mart_crm_opportunity.order_type LIKE ANY ('5.%', '6.%') THEN 'Churn'
      ELSE 'Other'
    END                                                                               AS trx_type,
    COALESCE (mart_crm_opportunity.opportunity_name LIKE '%Startups Program%', FALSE)                      AS startup_program_flag

  FROM mart_crm_opportunity
  LEFT JOIN dim_date
    ON mart_crm_opportunity.close_date = dim_date.date_actual
  LEFT JOIN dim_crm_user AS user
    ON mart_crm_opportunity.dim_crm_user_id = user.dim_crm_user_id

  WHERE ((mart_crm_opportunity.is_edu_oss = 1 AND mart_crm_opportunity.net_arr > 0) OR mart_crm_opportunity.is_edu_oss = 0)
    AND mart_crm_opportunity.is_jihu_account = FALSE
    AND mart_crm_opportunity.stage_name NOT LIKE '%Duplicate%'
    --and (opportunity_category is null or opportunity_category not like 'Decom%')
    AND partial_churn_0_narr_flag = FALSE
--and fiscal_year >= 2023
--and (is_won or (stage_name = '8-Closed Lost' and subscription_type = 'Renewal') or is_closed = False)

)

--select * from opportunity_data;
,

/*
We pull all the Pooled cases, using the record type ID.

We have to manually parse the Subject field to get the Trigger Type, hopefully this will go away in future iterations.

-No spam filter
-Trigger Type logic only valid for FY25 onwards
*/

-- get_case_owner_history as
-- (
-- select 
-- iff(first_value(case_history.old_value) over(partition by case_history.case_id order by case_history.field_modified_at asc) is null
-- or first_value(case_history.old_value) over(partition by case_history.case_id order by case_history.field_modified_at asc) = '00G8X000006WmU3UAK',
-- first_value(case_history.new_value) over(partition by case_history.case_id order by case_history.field_modified_at asc),
-- first_value(case_history.old_value) over(partition by case_history.case_id order by case_history.field_modified_at asc))
-- as dim_crm_user_id_first_owner,
-- date(first_value(case_history.field_modified_at) over(partition by case_history.case_id order by case_history.field_modified_at asc)) as first_change_date,
-- first_value(case_history.new_value) over(partition by case_history.case_id,date(case_history.field_modified_at) order by case_history.field_modified_at desc) as dim_crm_user_id_last_owner_on_date,
-- date(lag(case_history.field_modified_at,1) over(partition by case_history.case_id order by case_history.field_modified_at asc)) as prior_owner_change_date,
-- date(lead(case_history.field_modified_at,1) over(partition by case_history.case_id order by case_history.field_modified_at asc)) as next_owner_change_date,
-- --first_value(new_value) over(partition by case_id order by field_modified_at asc) as user_name_first_owner,
-- case_data.dim_crm_user_id as dim_crm_user_id_current_owner,
-- case_history.* EXCLUDE field_modified_at,
-- date(case_history.field_modified_at) as change_date
-- from case_history
-- inner join case_data on case_history.case_id = case_data.case_id
--  where 
--  case_history.case_field = 'Owner'
-- and case_history.data_type = 'EntityId'
-- --and case_id = '500PL000008CLxBYAW'
-- ),

-- case_owner_logic as
-- (
-- select
-- distinct
-- dim_date.date_actual,
-- case_data.case_id,
-- case_data.created_date::date as created_date,
-- get_case_owner_history.change_date,
-- get_case_owner_history.dim_crm_user_id_first_owner,
-- get_case_owner_history.first_change_date,
-- get_case_owner_history.prior_owner_change_date,
-- get_case_owner_history.next_owner_change_date,
-- get_case_owner_history.dim_crm_user_id_last_owner_on_date,
-- case 
-- when get_case_owner_history.case_id is null then case_data.dim_crm_user_id
-- else
-- case
-- when dim_date.date_actual < case_data.created_date then null
-- when dim_date.date_actual >= case_data.created_date and dim_date.date_actual < get_case_owner_history.first_change_date then dim_crm_user_id_first_owner
-- when dim_date.date_actual >= case_data.created_date and dim_date.date_actual >= get_case_owner_history.change_date and (dim_date.date_actual < get_case_owner_history.next_owner_change_date or get_case_owner_history.next_owner_change_date is null) then dim_crm_user_id_last_owner_on_date
-- else null end
-- end
-- as dim_crm_user_id_owner_on_date
-- from
-- dim_date
-- left join case_data
-- left join get_case_owner_history on get_case_owner_history.case_id = case_data.case_id
-- --and change_date = date_actual
-- where
-- dim_date.fiscal_year = 2025
-- --and case_id = '500PL000005mnhTYAQ'
-- order by 1 asc
-- ),

-- case_history_output as
-- (
-- select
-- distinct
-- date_actual,
-- case_id,
-- dim_crm_user_id_first_owner,
-- created_date,
-- dim_crm_user_id_owner_on_date
-- from
-- case_owner_logic
-- where
-- (dim_crm_user_id_owner_on_date is not null or date_actual < created_date)
-- order by 1 asc
-- ),

--SELET * from task_data; -- No records?
-- ,

-- I don't think this CTE is in use
-- opp AS (
--   SELECT *
--   FROM mart_crm_opportunity
--   WHERE ((mart_crm_opportunity.is_edu_oss = 1 AND net_arr > 0) OR mart_crm_opportunity.is_edu_oss = 0)
--     AND mart_crm_opportunity.is_jihu_account = FALSE
--     AND stage_name NOT LIKE '%Duplicate%'
--     AND (opportunity_category IS NULL OR opportunity_category NOT LIKE 'Decom%')
-- --and partial_churn_0_narr_flag = false
-- --and fiscal_year >= 2023
-- --and (is_won or (stage_name = '8-Closed Lost' and subscription_type = 'Renewal') or is_closed = False)
-- ),

opps_for_upgrades as
(
SELECT
*
FROM mart_crm_opportunity
WHERE 
((mart_crm_opportunity.is_edu_oss = 1 and net_arr > 0) or mart_crm_opportunity.is_edu_oss = 0)
AND 
mart_crm_opportunity.is_jihu_account = False
AND stage_name not like '%Duplicate%'
and (opportunity_category is null or opportunity_category not like 'Decom%')
--and partial_churn_0_narr_flag = false
--and fiscal_year >= 2023
--and (is_won or (stage_name = '8-Closed Lost' and subscription_type = 'Renewal') or is_closed = False)

),

upgrades_prep as
(
SELECT
  mart_delta_arr_subscription_month.arr_month,
  mart_delta_arr_subscription_month.type_of_arr_change,
  mart_delta_arr_subscription_month.product_category[0] as upgrade_product,
  mart_delta_arr_subscription_month.previous_month_product_category[0] as prior_product,
  opps_for_upgrades.is_web_portal_purchase,
  mart_delta_arr_subscription_month.dim_crm_account_id, 
  opps_for_upgrades.dim_crm_opportunity_id,
  SUM(mart_delta_arr_subscription_month.beg_arr)                      AS beg_arr,
  SUM(mart_delta_arr_subscription_month.end_arr)                      AS end_arr,
  SUM(mart_delta_arr_subscription_month.end_arr) - SUM(mart_delta_arr_subscription_month.beg_arr)       AS delta_arr,
  SUM(mart_delta_arr_subscription_month.seat_change_arr)              AS seat_change_arr,
  SUM(mart_delta_arr_subscription_month.price_change_arr)             AS price_change_arr,
  SUM(mart_delta_arr_subscription_month.tier_change_arr)              AS tier_change_arr,
  SUM(mart_delta_arr_subscription_month.beg_quantity)                 AS beg_quantity,
  SUM(mart_delta_arr_subscription_month.end_quantity)                 AS end_quantity,
  SUM(mart_delta_arr_subscription_month.seat_change_quantity)         AS delta_seat_change,
  COUNT(*)                          AS nbr_customers_upgrading
FROM mart_delta_arr_subscription_month
left join opps_for_upgrades
  on (mart_delta_arr_subscription_month.DIM_CRM_ACCOUNT_ID = opps_for_upgrades.DIM_CRM_ACCOUNT_ID
  and mart_delta_arr_subscription_month.arr_month = date_trunc('month',opps_for_upgrades.subscription_start_date)
 -- and opp.order_type = '3. Growth'
  and opps_for_upgrades.is_won)
WHERE 
 (ARRAY_CONTAINS('Self-Managed - Starter'::VARIANT, mart_delta_arr_subscription_month.previous_month_product_category)
          OR ARRAY_CONTAINS('SaaS - Bronze'::VARIANT, mart_delta_arr_subscription_month.previous_month_product_category)
       or ARRAY_CONTAINS('SaaS - Premium'::VARIANT, mart_delta_arr_subscription_month.previous_month_product_category)
       or ARRAY_CONTAINS('Self-Managed - Premium'::VARIANT, mart_delta_arr_subscription_month.previous_month_product_category)
       )
   AND mart_delta_arr_subscription_month.tier_change_arr > 0
GROUP BY 1,2,3,4,5,6,7
),

upgrades AS ( 
  select distinct dim_crm_opportunity_id from
  upgrades_prep
)

--select * from upgrades;
,

promo_data_actual_price AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    MAX(mart_charge.arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
    MAX(mart_charge.quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
    actual_arr / NULLIFZERO(actual_quantity)                          AS actual_price
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE
    -- effective_start_date >= '2023-02-01'
    -- and
    dim_subscription.subscription_start_date >= '2023-04-01'
    AND dim_subscription.subscription_start_date <= '2023-07-01'
    AND mart_charge.type_of_arr_change = 'New'
    AND mart_charge.quantity > 0
    -- and actual_price > 228
    -- and actual_price < 290
    AND mart_charge.rate_plan_charge_name LIKE '%Premium%'
  QUALIFY
    actual_price > 228
    AND actual_price < 290
),

price_increase_promo_fo_data AS (--Gets all FO opportunities associated with Price Increase promo

  SELECT DISTINCT
    dim_crm_opportunity_id,
    actual_price
  FROM promo_data_actual_price
)

--select * from price_increase_promo_fo_data;
,

bronz_starter_accounts AS (
  SELECT dim_crm_account_id
  --,product_rate_plan_name
  FROM mart_arr
  WHERE arr_month >= '2020-02-01'
    AND arr_month <= '2022-02-01'
    AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
),

eoa_accounts AS (
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
    COALESCE (mart_arr.contract_seat_reconciliation = 'Yes' AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE) AS qsr_enabled_flag,
    mart_arr.product_tier_name,
    mart_arr.product_delivery_type,
    mart_arr.product_rate_plan_name,
    mart_arr.arr,
    --,monthly_mart.max_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
    (mart_arr.arr / NULLIFZERO(mart_arr.quantity))                                                                   AS arr_per_user,
    arr_per_user / 12                                                                                                AS monthly_price_per_user,
    mart_arr.mrr / NULLIFZERO(mart_arr.quantity)                                                                     AS mrr_check
  FROM mart_arr
-- LEFT JOIN RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_ACCOUNT
--     ON mart_arr.DIM_CRM_ACCOUNT_ID = MART_CRM_ACCOUNT.DIM_CRM_ACCOUNT_ID
  LEFT JOIN bronz_starter_accounts
    ON mart_arr.dim_crm_account_id = bronz_starter_accounts.dim_crm_account_id
  WHERE mart_arr.arr_month = '2023-01-01'
    AND mart_arr.product_tier_name LIKE '%Premium%'
    AND ((
      monthly_price_per_user >= 14
      AND monthly_price_per_user <= 16
    ) OR (monthly_price_per_user >= 7.5 AND monthly_price_per_user <= 9.5
    ))
    AND bronz_starter_accounts.dim_crm_account_id IS NOT NULL
/*dim_crm_account_id IN
                    (
                      SELECT
                        dim_crm_account_id
--,product_rate_plan_name
                      FROM mart_arr
                      WHERE arr_month >= '2020-02-01'
                        AND arr_month <= '2022-02-01'
                        AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
                    )*/
--ORDER BY mart_arr.dim_crm_account_id ASC
),

eoa_accounts_fy24 AS (
--Looks for current month ARR around $15 to account for currency conversion.
--Checks to make sure accounts previously had ARR in Bronze or Starter (to exclude accounts that just have discounts)
  SELECT DISTINCT dim_crm_account_id
  FROM eoa_accounts
),

past_eoa_uplift_opportunities AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal,
    mart_charge.previous_mrr / NULLIFZERO(mart_charge.previous_quantity) AS previous_price,
    mart_charge.mrr / NULLIFZERO(mart_charge.quantity)                   AS price_after_renewal
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  INNER JOIN eoa_accounts_fy24
    ON mart_charge.dim_crm_account_id = eoa_accounts_fy24.dim_crm_account_id
  -- left join PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY on dim_subscription.dim_crm_opportunity_id_current_open_renewal = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE mart_charge.rate_plan_name LIKE '%Premium%'
    -- and
    -- mart_charge.term_start_date <= '2024-02-01'
    AND mart_charge.type_of_arr_change != 'New'
    AND mart_charge.term_start_date >= '2022-02-01'
    AND price_after_renewal > previous_price
    AND mart_charge.previous_quantity != 0
    AND mart_charge.quantity != 0
    AND (
      LOWER(mart_charge.rate_plan_charge_description) LIKE '%eoa%'
      OR
      (
        (previous_price >= 5 AND previous_price <= 7)
        OR (previous_price >= 8 AND previous_price <= 10)
        OR (previous_price >= 14 AND previous_price <= 16)
      )
    )
),

past_eoa_uplift_opportunity_data AS (--Gets all opportunities associated with EoA special pricing uplift
  SELECT DISTINCT dim_crm_opportunity_id
  FROM past_eoa_uplift_opportunities
)

--select * from past_eoa_uplift_opportunity_data;
,

free_limit_promo_fo_prep as
(
    SELECT
      mart_charge.*,
      dim_subscription.dim_crm_opportunity_id,
      MAX(mart_charge.arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
      MAX(mart_charge.quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
      actual_arr / NULLIFZERO(actual_quantity)                          AS actual_price
    FROM mart_charge
    LEFT JOIN dim_subscription
      ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    WHERE
      -- effective_start_date >= '2023-02-01'
      -- and
      mart_charge.rate_plan_charge_description LIKE '%70%'
      AND mart_charge.type_of_arr_change = 'New'
  ),


free_limit_promo_fo_data AS (--Gets all opportunities associated with Free Limit 70% discount

  SELECT DISTINCT
    dim_crm_opportunity_id
    -- ,
    -- actual_price
  FROM free_limit_promo_fo_prep
)

--select * from free_limit_promo_fo_data;
,

discounted_accounts AS (
  SELECT DISTINCT dim_crm_account_id
  FROM mart_charge
  WHERE subscription_start_date >= '2023-02-01'
    AND rate_plan_charge_description = 'fo-discount-70-percent'
),

open_eoa_renewals AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    close_date,
    MAX(mart_charge.arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
    MAX(mart_charge.quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
    actual_arr / NULLIFZERO(actual_quantity)                                      AS actual_price,
    previous_mrr / NULLIFZERO(previous_quantity)                                  AS previous_price,
    mrr / NULLIFZERO(quantity)                                                    AS price_after_renewal,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id_current_open_renewal = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN discounted_accounts
    ON mart_charge.dim_crm_account_id = discounted_accounts.dim_crm_account_id
  WHERE mart_crm_opportunity.close_date >= '2024-02-01'
    AND mart_crm_opportunity.is_closed = FALSE
    AND mart_charge.rate_plan_name LIKE '%Premium%'
    -- and
    -- mart_charge.term_start_date <= '2024-02-01'
    AND mart_charge.type_of_arr_change != 'New'
    AND mart_charge.term_start_date >= '2022-02-01'
    AND price_after_renewal > previous_price
    AND mart_charge.previous_quantity != 0
    AND mart_charge.quantity != 0
    AND discounted_accounts.dim_crm_account_id IS NULL
    /*mart_charge.dim_crm_account_id NOT IN
              (
                SELECT DISTINCT
                  dim_crm_account_id
                FROM restricted_safe_common_mart_sales.mart_charge charge
                WHERE subscription_start_date >= '2023-02-01'
                  AND rate_plan_charge_description = 'fo-discount-70-percent'
              )*/
    AND (
      LOWER(mart_charge.rate_plan_charge_description) LIKE '%eoa%'
      OR
      (
        (previous_price >= 5 AND previous_price <= 7)
        OR (previous_price >= 8 AND previous_price <= 10)
        OR (previous_price >= 14 AND previous_price <= 16)
      )
    )
),

open_eoa_renewal_data AS (
--Gets all future renewal opportunities where the account currently has EoA special pricing


  SELECT DISTINCT
    dim_crm_opportunity_id_current_open_renewal
    -- ,
    -- price_after_renewal,
    -- close_date
  FROM open_eoa_renewals


)

--select * from open_eoa_renewal_data;
,
renewal_self_service_data AS (--Uses the Order Action to determine if a Closed Won renewal was Autorenewed, Sales-Assisted, or Manual Portal Renew by the customer

  SELECT
    -- dim_order_action.dim_subscription_id,
    -- dim_subscription.subscription_name,
    -- dim_order_action.contract_effective_date,
    COALESCE (dim_order.order_description != 'AutoRenew by CustomersDot', FALSE) AS manual_portal_renew_flag,
    -- mart_crm_opportunity.is_web_portal_purchase,
    mart_crm_opportunity.dim_crm_opportunity_id,
    CASE
      WHEN manual_portal_renew_flag AND mart_crm_opportunity.is_web_portal_purchase THEN 'Manual Portal Renew'
      WHEN manual_portal_renew_flag = FALSE AND mart_crm_opportunity.is_web_portal_purchase THEN 'Autorenew'
      ELSE 'Sales-Assisted Renew'
    END                                                                AS actual_manual_renew_flag
  FROM dim_order_action
  LEFT JOIN dim_order
    ON dim_order_action.dim_order_id = dim_order.dim_order_id
  LEFT JOIN dim_subscription
    ON dim_order_action.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE dim_order_action.order_action_type = 'RenewSubscription'
)

--select * from renewal_self_service_data;
,

price_increase_promo_renewal_open AS (
  SELECT
    charge.*,
    charge.arr / NULLIFZERO(charge.quantity)     AS actual_price,
    prod.annual_billing_list_price AS list_price,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal
  FROM mart_charge AS charge
  INNER JOIN dim_product_detail AS prod
    ON charge.dim_product_detail_id = prod.dim_product_detail_id
  INNER JOIN dim_subscription
    ON charge.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE charge.term_start_date >= '2023-04-01'
    AND charge.term_start_date <= '2024-05-01'
    AND charge.type_of_arr_change != 'New'
    AND charge.subscription_start_date < '2023-04-01'
    AND charge.quantity > 0
    AND actual_price > 228
    AND actual_price < 290
    AND charge.rate_plan_charge_name LIKE '%Premium%'
),

price_increase_promo_renewal_open_data AS (
  SELECT DISTINCT dim_crm_opportunity_id_current_open_renewal
  FROM price_increase_promo_renewal_open

)
--SELECT * FROM price_increase_promo_renewal_open_data;

,

last_carr_data AS (
  SELECT
    snapshot_date,
    dim_crm_account_id,
    MAX(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END)
      OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_carr_date,
    MIN(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC)                                               AS first_carr_date
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date >= '2019-12-01'
    AND crm_account_type != 'Prospect'
  -- and crm_account_name like 'Natural Substances%'
  QUALIFY last_carr_date IS NOT NULL
)
--SELECT * FROM last_carr_data;
,
dim_subscription_prep as
(select
distinct
dim_crm_opportunity_id_current_open_renewal, 
first_value(turn_on_auto_renewal) over(partition by dim_crm_opportunity_id_current_open_renewal order by turn_on_auto_renewal asc) as TURN_ON_AUTO_RENEWAL
from dim_subscription
where dim_crm_opportunity_id_current_open_renewal is not null)
,


full_base_data AS (
  SELECT
    opportunity_data.*,
    opportunity_data.ptc_predicted_arr                                                                                                AS ptc_predicted_arr__c,
    opportunity_data.ptc_predicted_renewal_risk_category                                                                              AS ptc_predicted_renewal_risk_category__c,
    -- upgrades.prior_product,
    -- upgrades.upgrade_product,
    COALESCE (upgrades.dim_crm_opportunity_id IS NOT NULL, FALSE)                                                    AS upgrade_flag,
    COALESCE (price_increase_promo_fo_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                                AS price_increase_promo_fo_flag,
    COALESCE (free_limit_promo_fo_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                                    AS free_limit_promo_fo_flag,
    COALESCE (past_eoa_uplift_opportunity_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                            AS past_eoa_uplift_opportunity_flag,
    COALESCE (open_eoa_renewal_data.dim_crm_opportunity_id_current_open_renewal IS NOT NULL, FALSE)                  AS open_eoa_renewal_flag,
    COALESCE (price_increase_promo_renewal_open_data.dim_crm_opportunity_id_current_open_renewal IS NOT NULL, FALSE) AS price_increase_promo_open_renewal_flag,
    renewal_self_service_data.actual_manual_renew_flag                                                               AS actual_manual_renew_flag,
    dim_subscription_prep.turn_on_auto_renewal,
    last_carr_data.last_carr_date,
    last_carr_data.first_carr_date,
    last_carr_data.snapshot_date                                                                                     AS last_carr_snapshot_date,
    opportunity_data.arr_basis                                                                                                        AS atr,
    --case_history_output.dim_crm_user_id_owner_on_date,
    -- dim_crm_user.user_name                                                                                           AS case_owner_name_on_date,
    CASE
      WHEN opportunity_data.trx_type = 'First Order' OR opportunity_data.order_type_current LIKE '%First Order%' THEN 'First Order'
      WHEN opportunity_data.trx_type IN ('Renewal - Uplift', 'Renewal - Flat') THEN 'Renewal Growth'
      WHEN opportunity_data.qsr_flag OR opportunity_data.trx_type = 'Growth - Uplift' THEN 'Nonrenewal Growth'
      WHEN opportunity_data.trx_type IN ('Churn', 'Renewal - Contraction', 'Non-Renewal - Contraction') THEN 'C&C'
      ELSE 'Other'
    END                                                                                                              AS trx_type_grouping,
    wk_sales_gds_fo_buckets.bucket AS fo_source_bucket_name,
    case 
      when opportunity_data.is_closed and opportunity_data.order_type LIKE '%First Order%' then first_carr_date
      when opportunity_data.is_closed and opportunity_data.order_type NOT LIKE '%First Order%' then last_carr_date
      else CURRENT_DATE end as acct_snapshot_join_date

  FROM opportunity_data
  LEFT JOIN last_carr_data
    ON opportunity_data.dim_crm_account_id = last_carr_data.dim_crm_account_id
      AND ((
        opportunity_data.order_type NOT LIKE '%First Order%'
        AND opportunity_data.is_closed
        AND
        last_carr_data.snapshot_date = opportunity_data.close_date - 1
      )
      OR
      (
        opportunity_data.order_type LIKE '%First Order%'
        AND opportunity_data.is_closed
        AND last_carr_data.snapshot_date = last_carr_data.first_carr_date
      )
      OR
      (opportunity_data.is_closed = FALSE AND last_carr_data.snapshot_date = CURRENT_DATE)
      )
  LEFT JOIN upgrades
    ON opportunity_data.dim_crm_opportunity_id = upgrades.dim_crm_opportunity_id
  LEFT JOIN price_increase_promo_fo_data
    ON opportunity_data.dim_crm_opportunity_id = price_increase_promo_fo_data.dim_crm_opportunity_id
  LEFT JOIN past_eoa_uplift_opportunity_data
    ON opportunity_data.dim_crm_opportunity_id = past_eoa_uplift_opportunity_data.dim_crm_opportunity_id
  LEFT JOIN free_limit_promo_fo_data
    ON opportunity_data.dim_crm_opportunity_id = free_limit_promo_fo_data.dim_crm_opportunity_id
  LEFT JOIN open_eoa_renewal_data
    ON opportunity_data.dim_crm_opportunity_id = open_eoa_renewal_data.dim_crm_opportunity_id_current_open_renewal
  LEFT JOIN renewal_self_service_data
    ON opportunity_data.dim_crm_opportunity_id = renewal_self_service_data.dim_crm_opportunity_id
  LEFT JOIN price_increase_promo_renewal_open_data
    ON opportunity_data.dim_crm_opportunity_id
      = price_increase_promo_renewal_open_data.dim_crm_opportunity_id_current_open_renewal
  -- LEFT JOIN case_history_output on case_history_output.case_id = opportunity_data.high_value_case_id
  -- and case_history_output.date_actual = opportunity_data.close_date
  -- LEFT JOIN dim_crm_user
  --   ON case_history_output.dim_crm_user_id_owner_on_date = dim_crm_user.dim_crm_user_id
  LEFT JOIN wk_sales_gds_fo_buckets
    on opportunity_data.dim_crm_opportunity_id = wk_sales_gds_fo_buckets.dim_crm_opportunity_id
  LEFT JOIN  dim_subscription_prep
    ON opportunity_data.dim_crm_opportunity_id = dim_subscription_prep.dim_crm_opportunity_id_current_open_renewal
)


{{ dbt_audit(
    cte_ref="full_base_data",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2024-07-15"
) }}