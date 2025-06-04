{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([

    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_arr', 'mart_arr'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges'),
    ('dim_date', 'dim_date'),
    ('dim_product_detail', 'dim_product_detail'),
    ('mart_charge', 'mart_charge'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_billing_account', 'dim_billing_account'),
    ('mart_product_usage_paid_user_metrics_monthly', 'mart_product_usage_paid_user_metrics_monthly'),
    ('prep_billing_account_user', 'prep_billing_account_user'),
    ('wk_sales_gds_cases', 'wk_sales_gds_cases'),
    ('customers_db_customers_source', 'customers_db_customers_source')
]) }},

prep_crm_case AS (
  SELECT * FROM {{ ref('prep_crm_case') }}
  WHERE is_deleted = FALSE
),


mart_crm_account AS (
  SELECT * FROM {{ ref('mart_crm_account') }}
  WHERE is_deleted = FALSE
),


dim_crm_account_daily_snapshot AS (
  SELECT *
  FROM {{ ref('dim_crm_account_daily_snapshot') }}
  WHERE snapshot_date = CURRENT_DATE
    AND is_deleted = FALSE
),

dim_subscription_snapshot_bottom_up AS (
  SELECT *
  FROM {{ ref('dim_subscription_snapshot_bottom_up') }}
  WHERE snapshot_id >= 20240201
),

sfdc_contact_snapshots_source AS (
  SELECT *
  FROM {{ ref('sfdc_contact_snapshots_source') }}
  WHERE dbt_valid_from::DATE >= '2024-02-01'
),

mart_behavior_structured_event AS (
  SELECT *
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE behavior_date >= '2024-07-20'
    AND event_category = 'Webstore'
),

auto_renew_will_fail_one AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    product_rate_plan_name,
    subscription_end_month                                                    AS auto_renewal_sub_end_month,
    turn_on_auto_renewal,
    COUNT(DISTINCT product_tier_name) OVER (PARTITION BY dim_subscription_id) AS sub_prod_count
  FROM mart_arr
  WHERE (
    arr_month = DATE_TRUNC('month', CURRENT_DATE())
    OR arr_month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  )
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


-- Pulls existing Duo Trial lead cases
existing_duo_trial AS (
  SELECT dim_crm_account_id
  FROM prep_crm_case
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND subject LIKE 'Duo Trial Started%'
),

-- Identifies which subscriptions have Duo on them
duo_on_sub AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_subscription_id,
    product_rate_plan_name,
    subscription_end_month                                          AS auto_renewal_sub_end_month,
    turn_on_auto_renewal,
    COALESCE(CONTAINS(LOWER(product_rate_plan_name), 'duo'), FALSE) AS duo_flag
  FROM mart_arr_all
  WHERE (
    arr_month = DATE_TRUNC('month', CURRENT_DATE())
    OR arr_month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  )
  AND duo_flag = TRUE
),

-- Pulls information about the most recent First Order on each account
-- , first_order AS (
--     SELECT DISTINCT
--         mart_crm_opportunity.dim_crm_account_id,
--         LAST_VALUE(mart_crm_opportunity.net_arr)
--             OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
--             AS net_arr,
--         LAST_VALUE(mart_crm_opportunity.close_date)
--             OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
--             AS close_date,
--         LAST_VALUE(dim_date.fiscal_year)
--             OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
--             AS fiscal_year,
--         LAST_VALUE(mart_crm_opportunity.sales_qualified_source_name)
--             OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
--             AS sqs,
--         LAST_VALUE(mart_crm_opportunity.opportunity_name)
--             OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id ORDER BY mart_crm_opportunity.close_date ASC)
--             AS fo_opp_name
--     FROM mart_crm_opportunity
--     LEFT JOIN dim_date
--         ON mart_crm_opportunity.close_date = dim_date.date_actual
--     WHERE mart_crm_opportunity.is_won
--         AND mart_crm_opportunity.order_type = '1. New - First Order'
-- )

-- Pulls information about the most recent Churn on each account
-- , latest_churn AS (
--     SELECT
--         snapshot_date AS close_date,
--         dim_crm_account_id,
--         carr_this_account,
--         LAG(carr_this_account, 1) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC) AS prior_carr,
--         -prior_carr AS net_arr,
--         MAX(CASE
--             WHEN carr_this_account > 0 THEN snapshot_date
--         END) OVER (PARTITION BY dim_crm_account_id) AS last_carr_date
--     FROM dim_crm_account_daily_snapshot
--     WHERE snapshot_date >= '2019-02-01'
--         AND snapshot_date = DATE_TRUNC('month', snapshot_date)
--     QUALIFY
--         prior_carr > 0
--         AND carr_this_account = 0
--         AND snapshot_date > last_carr_date
-- )



-- Identifies accounts that had their First Order discounted 70% as part of the Free User limits promotion
-- , free_promo AS (
--     SELECT DISTINCT dim_crm_account_id
--     FROM mart_charge
--     WHERE subscription_start_date >= '2023-02-01'
--         AND rate_plan_charge_description = 'fo-discount-70-percent'
-- )

-- , price_increase_prep AS (
--     SELECT
--         mart_charge.*,
--         DIV0(mart_charge.arr, mart_charge.quantity) AS actual_price,
--         prod.annual_billing_list_price AS list_price
--     FROM mart_charge
--     INNER JOIN dim_product_detail AS prod
--         ON mart_charge.dim_product_detail_id = prod.dim_product_detail_id
--     WHERE mart_charge.subscription_start_date >= '2023-04-01'
--         AND mart_charge.subscription_start_date <= '2023-07-01'
--         AND mart_charge.type_of_arr_change = 'New'
--         AND mart_charge.quantity > 0
--         AND actual_price > 228
--         AND actual_price < 290
--         AND mart_charge.rate_plan_charge_name LIKE '%Premium%'
-- )

-- Identifies accounts that received a First Order discount as part of the Premium price increase promotion
-- , price_increase AS (
--     SELECT DISTINCT dim_crm_account_id
--     FROM price_increase_prep
-- )

-- Identifies any accounts with Ultimate ARR
-- , ultimate AS (
--     SELECT DISTINCT
--         dim_crm_account_id,
--         arr_month,
--         MAX(arr_month) OVER (PARTITION BY dim_crm_account_id ORDER BY arr_month DESC) AS last_ultimate_arr_month
--     FROM mart_arr
--     WHERE product_tier_name LIKE '%Ultimate%'
--         AND arr > 0
--     QUALIFY (arr_month = last_ultimate_arr_month AND arr_month <= DATE_TRUNC('month', CURRENT_DATE))
--         OR (last_ultimate_arr_month > DATE_TRUNC('month', CURRENT_DATE) AND arr_month = DATE_TRUNC('month', CURRENT_DATE))
-- )

-- All AMER accounts
amer_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN ('AMER SMB Sales')
    OR owner_role = 'Advocate_SMB_AMER'
),

-- All EMEA/APJ team accounts
emea_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN ('EMEA SMB Sales', 'APAC SMB Sales', 'APJ SMB Sales')
    OR (owner_role = 'Advocate_SMB_EMEA' OR owner_role = 'Advocate_SMB_APAC' OR owner_role = 'Advocate_SMB_APJ')
),

account_base AS (
  SELECT
    acct.*,
    CASE
      WHEN amer_accounts.dim_crm_account_id IS NOT NULL THEN 'AMER'
      WHEN emea_accounts.dim_crm_account_id IS NOT NULL THEN 'EMEA'
      ELSE 'Other'
    END AS team
  FROM dim_crm_account_daily_snapshot AS acct
  LEFT JOIN amer_accounts
    ON acct.dim_crm_account_id = amer_accounts.dim_crm_account_id
  LEFT JOIN emea_accounts
    ON acct.dim_crm_account_id = emea_accounts.dim_crm_account_id
  WHERE acct.snapshot_date = CURRENT_DATE
    AND (amer_accounts.dim_crm_account_id IS NOT NULL OR emea_accounts.dim_crm_account_id IS NOT NULL)
),

billing_accounts AS (
  SELECT DISTINCT
    dim_crm_account_id,
    po_required
  FROM dim_billing_account
  WHERE po_required = 'YES'
),

account_blended AS (
  SELECT
    DATEADD('day', 30, CURRENT_DATE)                                                                             AS current_date_30_days,
    DATEADD('day', 60, CURRENT_DATE)                                                                             AS current_date_60_days,
    DATEADD('day', 80, CURRENT_DATE)                                                                             AS current_date_80_days,
    DATEADD('day', 90, CURRENT_DATE)                                                                             AS current_date_90_days,
    DATEADD('day', 180, CURRENT_DATE)                                                                            AS current_date_180_days,
    DATEADD('day', 270, CURRENT_DATE)                                                                            AS current_date_270_days,
    account_base.dim_crm_account_id                                                                              AS account_id,
    account_base.account_owner,
    account_base.user_role_type,
    account_base.crm_account_owner_geo,
    account_base.parent_crm_account_sales_segment,
    account_base.parent_crm_account_business_unit,
    account_base.next_renewal_date,
    account_base.team,
    account_base.count_active_subscriptions,
    DATEDIFF('day', CURRENT_DATE, account_base.next_renewal_date)                                                AS days_till_next_renewal,
    account_base.carr_account_family,
    account_base.carr_this_account,
    account_base.parent_crm_account_lam_dev_count,
    account_base.parent_crm_account_max_family_employee,
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
    -- account_base.free_promo_flag,
    -- account_base.price_increase_promo_flag,
    DATEDIFF('day', account_base.six_sense_account_update_date, CURRENT_DATE)                                    AS days_since_6sense_account_update,
    mart_crm_opportunity.dim_crm_opportunity_id                                                                  AS opportunity_id,
    mart_crm_opportunity.owner_id                                                                                AS opportunity_owner_id,
    COALESCE(
      mart_crm_opportunity.subscription_type = 'Renewal'
      AND mart_crm_opportunity.close_date >= '2025-02-01'
      AND mart_crm_opportunity.close_date <= '2026-01-31',
      FALSE
    )                                                                                                            AS fy26_renewal,
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
    CASE
      WHEN dim_crm_opportunity.payment_schedule LIKE 'AWS%' THEN 'AWS'
      WHEN dim_crm_opportunity.payment_schedule LIKE 'GCP%' THEN 'GCP'
    END                                                                                                          AS private_offer_flag,
    COALESCE(mart_crm_opportunity.deal_path_name = 'Partner', FALSE)                                             AS partner_opp_flag,
    dim_subscription.dim_subscription_id                                                                         AS sub_subscription_id,
    dim_subscription.dim_subscription_id_original                                                                AS sub_subscription_id_original,
    dim_subscription.subscription_name                                                                           AS sub_subscription_name,
    dim_subscription.subscription_status,
    dim_subscription.term_start_date                                                                             AS current_subscription_start_date,
    dim_subscription.term_end_date                                                                               AS current_subscription_end_date,
    DATEDIFF('day', dim_subscription.term_start_date, CURRENT_DATE)                                              AS days_into_current_subscription,
    dim_subscription.turn_on_auto_renewal,
    COUNT(DISTINCT dim_subscription.dim_subscription_id) OVER (PARTITION BY dim_subscription.dim_crm_account_id) AS count_subscriptions,
    dim_subscription.dim_subscription_id,
    CASE
      WHEN mart_crm_opportunity.subscription_type = 'Renewal' THEN DATEDIFF('day', CURRENT_DATE, mart_crm_opportunity.close_date)
    END                                                                                                          AS days_till_close,
    billing_accounts.po_required,
    mart_crm_opportunity.auto_renewal_status,
    COALESCE(auto_renew_will_fail.dim_crm_account_id IS NOT NULL, FALSE)                                         AS auto_renew_will_fail_mult_products,
    COALESCE(duo_on_sub.dim_crm_account_id IS NOT NULL, FALSE)                                                   AS duo_on_sub_flag,
    COALESCE(
      (
        CONTAINS(mart_crm_opportunity.opportunity_name, '#ultimateupgrade')
        OR CONTAINS(mart_crm_opportunity.opportunity_name, 'Ultimate Upgrade')
        OR CONTAINS(mart_crm_opportunity.opportunity_name, 'Upgrade to Ultimate')
        OR CONTAINS(mart_crm_opportunity.opportunity_name, 'ultimate upgrade')
      ),
      FALSE
    )                                                                                                            AS ultimate_upgrade_oppty_flag,
    COALESCE(existing_duo_trial.dim_crm_account_id IS NOT NULL, FALSE)                                           AS existing_duo_trial_flag
  FROM account_base
  LEFT JOIN mart_crm_opportunity
    ON account_base.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
      AND DATEDIFF('day', CURRENT_DATE, mart_crm_opportunity.close_date) <= 365
      AND mart_crm_opportunity.is_closed = FALSE
  LEFT JOIN dim_subscription
    ON mart_crm_opportunity.dim_crm_opportunity_id = dim_subscription.dim_crm_opportunity_id_current_open_renewal
      AND mart_crm_opportunity.is_closed = FALSE
  LEFT JOIN dim_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN billing_accounts
    ON account_base.dim_crm_account_id = billing_accounts.dim_crm_account_id
  LEFT JOIN auto_renew_will_fail
    ON account_base.dim_crm_account_id = auto_renew_will_fail.dim_crm_account_id
      AND dim_subscription.dim_subscription_id = auto_renew_will_fail.dim_subscription_id
  LEFT JOIN existing_duo_trial
    ON account_base.dim_crm_account_id = existing_duo_trial.dim_crm_account_id
  LEFT JOIN duo_on_sub
    ON account_base.dim_crm_account_id = duo_on_sub.dim_crm_account_id
      AND dim_subscription.dim_subscription_id = duo_on_sub.dim_subscription_id
  WHERE (
    mart_crm_opportunity.subscription_type != 'Renewal'
    OR (
      mart_crm_opportunity.subscription_type = 'Renewal'
      AND LOWER(mart_crm_opportunity.opportunity_name) NOT LIKE '%edu program%'
      AND LOWER(mart_crm_opportunity.opportunity_name) NOT LIKE '%oss program%'
    )
  )
  AND mart_crm_opportunity.is_closed = FALSE
),

-- Current month basic user count information
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

-- Calculates utilization at the user/account level, pricing, quantity
utilization AS (
  SELECT DISTINCT
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
    COALESCE (mart_arr.contract_seat_reconciliation = 'Yes'
    AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE)                                                         AS qsr_enabled_flag,
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
    MAX(snapshot_month) OVER (PARTITION BY monthly_mart.dim_subscription_id_original)                                AS latest_overage_month,
    MAX(arr_month) OVER (PARTITION BY mart_arr.dim_crm_account_id)                                                   AS latest_arr_month
  FROM {{ ref('mart_arr') }}
  LEFT JOIN (
    SELECT DISTINCT
      snapshot_month,
      dim_subscription_id_original,
      instance_type,
      subscription_status,
      subscription_start_date,
      subscription_end_date,
      term_end_date,
      license_user_count,
      MAX(billable_user_count) OVER (PARTITION BY dim_subscription_id_original) AS max_billable_user_count,
      MAX(ping_created_at) OVER (PARTITION BY dim_crm_account_id)               AS latest_ping
    FROM {{ ref('mart_product_usage_paid_user_metrics_monthly') }}
    WHERE instance_type = 'Production'
      AND subscription_status = 'Active'
      AND is_latest_data
  ) AS monthly_mart
    ON mart_arr.dim_subscription_id_original = monthly_mart.dim_subscription_id_original
  WHERE product_tier_name LIKE ANY ('%Premium%', '%Ultimate%')
    AND quantity > 0
  QUALIFY arr_month = latest_arr_month
),

cancel_events AS (
  SELECT
    *,
    contexts['data'][0]['data']['customer_id'] AS customer_id
  FROM mart_behavior_structured_event
  WHERE behavior_date >= '2024-07-20'
    AND event_category = 'Webstore'
    AND event_action LIKE 'cancel%'
    AND (event_property != 'Testing Purpose' OR event_property IS NULL)
),

portal_cancel_reasons AS (
  SELECT
    cancel_events.*,
    customers_db_customers_source.sfdc_account_id AS dim_crm_account_id
  FROM cancel_events
  LEFT JOIN customers_db_customers_source
    ON cancel_events.customer_id = customers_db_customers_source.customer_id
),

-- Identifies accounts where a user has manually switched off autorenew (canceled subscription)
auto_renew_switch_one AS (
  SELECT
    dim_date.date_actual                                                                                          AS snapshot_date,
    prep_billing_account_user.user_name                                                                           AS update_user,
    prep_billing_account_user.is_integration_user,
    dim_subscription_snapshot_bottom_up.turn_on_auto_renewal                                                      AS active_autorenew_status,
    LAG(dim_subscription_snapshot_bottom_up.turn_on_auto_renewal, 1)
      OVER (PARTITION BY dim_subscription_snapshot_bottom_up.subscription_name ORDER BY dim_date.date_actual ASC)
      AS prior_auto_renewal,
    LEAD(dim_subscription_snapshot_bottom_up.turn_on_auto_renewal, 1)
      OVER (PARTITION BY dim_subscription_snapshot_bottom_up.subscription_name ORDER BY dim_date.date_actual ASC)
      AS future_auto_renewal,
    dim_subscription_snapshot_bottom_up.*,
    portal_cancel_reasons.event_label                                                                             AS cancel_reason,
    portal_cancel_reasons.event_property                                                                          AS cancel_comments
  FROM dim_subscription_snapshot_bottom_up
  INNER JOIN dim_date
    ON dim_subscription_snapshot_bottom_up.snapshot_id = dim_date.date_id
  INNER JOIN prep_billing_account_user
    ON dim_subscription_snapshot_bottom_up.updated_by_id = prep_billing_account_user.zuora_user_id
  LEFT JOIN portal_cancel_reasons
    ON dim_subscription_snapshot_bottom_up.dim_crm_account_id = portal_cancel_reasons.dim_crm_account_id
  WHERE snapshot_date >= '2023-02-01'
    AND snapshot_date < CURRENT_DATE
    AND dim_subscription_snapshot_bottom_up.subscription_status = 'Active'
  ORDER BY 1 ASC
),

autorenew_switch AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    active_autorenew_status,
    prior_auto_renewal,
    cancel_reason,
    cancel_comments,
    MAX(snapshot_date) AS latest_switch_date
  FROM auto_renew_switch_one
  WHERE (
    (active_autorenew_status = 'No' AND update_user = 'svc_zuora_fulfillment_int@gitlab.com')
    AND prior_auto_renewal = 'Yes'
  )
  GROUP BY 1, 2, 3, 4, 5, 6
),

-- Brings together account blended date, utilization data, and autorenewal switch data
all_data AS (
  SELECT
    account_blended.*,
    CASE
      WHEN utilization.subscription_end_month > '2024-04-01' AND utilization.product_tier_name LIKE '%Premium%' THEN 29
      WHEN utilization.product_tier_name LIKE '%Ultimate%' THEN 1188 / 12
      ELSE 0
    END                                                                AS future_price,
    utilization.overage_count,
    utilization.overage_amount,
    utilization.latest_overage_month,
    utilization.qsr_enabled_flag,
    utilization.max_billable_user_count,
    utilization.license_user_count,
    utilization.arr,
    utilization.arr_per_user,
    utilization.turn_on_seat_reconciliation,
    autorenew_switch.latest_switch_date,
    DATEDIFF('day', autorenew_switch.latest_switch_date, CURRENT_DATE) AS days_since_autorenewal_switch,
    autorenew_switch.cancel_reason,
    autorenew_switch.cancel_comments
  FROM account_blended
  LEFT JOIN utilization
    ON account_blended.account_id = utilization.dim_crm_account_id
      AND account_blended.sub_subscription_id_original = utilization.dim_subscription_id_original
  LEFT JOIN autorenew_switch
    ON account_blended.account_id = autorenew_switch.dim_crm_account_id
      AND account_blended.sub_subscription_id = autorenew_switch.dim_subscription_id
),

---this CTE functions as the case_flag CTE but for auto-renew-off cases only. This is where any additional case flags should be added in if more cases are going to be added to this query
auto_renew_off_case_flag AS (
  SELECT
    *,
    COALESCE (latest_switch_date IS NOT NULL, FALSE) AS auto_renew_recently_turned_off_flag
  FROM all_data
  WHERE is_closed = FALSE
),

-----this CTE functions as the "final" CTE in previous version - this is where the final case outputs will come out based on the case flags in the previous CTE. If there needs to be any prioritization done between case types it should be done here. Additional cases can be added into this CTE
auto_renew_cases AS (
  SELECT
    *,
    CASE
      WHEN auto_renew_recently_turned_off_flag = TRUE
        AND latest_switch_date <= CURRENT_DATE
        AND latest_switch_date >= DATEADD('day', -7, CURRENT_DATE)
        AND arr_basis > 3000
        THEN 'Auto-Renew Recently Turned Off'
    END AS case_trigger
  FROM auto_renew_off_case_flag
),

----this CTE will get the fields needed in the final output
distinct_cases AS (
  SELECT DISTINCT
    final.case_trigger,
    final.account_id,
    COALESCE(ROUND(final.arr_per_user, 2), 0) AS current_price,
    COALESCE(final.future_price * 12, 0)      AS renewal_price,
    case_data.case_trigger_id,
    case_data.status,
    case_data.case_origin,
    case_data.type,
    CASE
      WHEN case_data.case_trigger_id IN (6)
        THEN COALESCE(CONCAT(case_data.case_subject, ' ', final.latest_switch_date), case_data.case_subject)
    END                                       AS case_subject,
    case_data.case_reason,
    case_data.record_type_id,
    case_data.priority,
    case_data.case_cta,
    CASE
      WHEN case_data.case_trigger_id = 6
        THEN '- Criteria: Customer turned off autorenew in the last few days, with CARR > $3000'
      ELSE 'FIX CRITERIA'
    END                                       AS case_criteria,
    CONCAT(
      '- AWS/GCP Private Offer: ',
      COALESCE(final.private_offer_flag::VARCHAR, ''),
      ' | Renewal Date: ',
      COALESCE(final.close_date::VARCHAR, ''),
      ' | Current Price: ',
      COALESCE(current_price, 0),
      ' | Renewal Price: ',
      COALESCE(future_price, 0),
      ' | Current Licenses: ',
      COALESCE(final.license_user_count, 0),
      ' | Billable Users: ',
      COALESCE(final.max_billable_user_count, 0),
      ' | ARR increase due to price/quantity: $',
      COALESCE(future_price * final.max_billable_user_count - current_price * final.license_user_count, 0)::INT
    )                                         AS renewal_standard_context,
    CONCAT(
      '- Autorenewal: ',
      COALESCE(final.turn_on_auto_renewal, ''),
      ' | Autorenewal Status (indicates issue): ',
      COALESCE(final.auto_renewal_status, ''),
      ' | Date autorenewal canceled: ',
      COALESCE(final.latest_switch_date::VARCHAR, ''),
      ' | Cancellation Reason: ',
      COALESCE(final.cancel_reason, ''),
      ' | Cancellation Comments: ',
      COALESCE(final.cancel_comments, '')
    )                                         AS autorenewal_context,
    CASE
      WHEN case_data.case_trigger_id IN (6)
        THEN
          CONCAT(
            case_criteria,
            CHAR(10),
            renewal_standard_context,
            CHAR(10),
            autorenewal_context
          )
      ELSE 'FIX CONTEXT'
    END                                       AS context
  FROM auto_renew_cases AS final
  LEFT JOIN prod.boneyard.case_data AS case_data
    ON final.case_trigger = case_data.case_trigger
  WHERE final.case_trigger IS NOT NULL
),

case_output AS (
  SELECT
    distinct_cases.*,
    CURRENT_DATE AS query_run_date
  FROM distinct_cases
  LEFT JOIN prep_crm_case
    ON distinct_cases.account_id = prep_crm_case.dim_crm_account_id
      AND distinct_cases.case_subject = prep_crm_case.subject
  WHERE prep_crm_case.dim_crm_account_id IS NULL
    AND distinct_cases.case_subject NOT LIKE '%2023%'
    AND distinct_cases.case_subject NOT LIKE '%2024%'
    AND distinct_cases.case_subject IS NOT NULL
)

{{ dbt_audit(
    cte_ref="case_output",
    created_by="@sglad",
    updated_by="@sglad",
    created_date="2024-07-02",
    updated_date="2025-02-21"
) }}
