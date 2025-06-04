WITH

dim_crm_user AS (
  SELECT * 
  FROM {{ ref('dim_crm_user') }}
),

mart_arr_all AS (
  SELECT * 
  FROM {{ ref('mart_arr_with_zero_dollar_charges') }}
),
--test
billing_accounts AS (
  SELECT DISTINCT
    dim_crm_account_id,
    po_required
  FROM {{ ref('dim_billing_account') }} 
  WHERE po_required = 'YES'
),

qsr_failed_one AS (
  SELECT DISTINCT
    dim_crm_account_id,
    dim_crm_opportunity_id,
    COALESCE(CONTAINS(LOWER(subject), 'failed qsr'), FALSE) AS qsr_failed_last_75
  FROM {{ ref('prep_crm_case') }}
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND CONTAINS(LOWER(subject), 'failed qsr')
),

qsr_failed_last_75 AS (
  SELECT DISTINCT
    dim_crm_account_id,
--    dim_crm_opportunity_id,
    TRUE AS qsr_failed_last_75
  FROM qsr_failed_one
  WHERE qsr_failed_last_75
),

auto_renew_will_fail_one AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    product_rate_plan_name,
    subscription_end_month                                                    AS auto_renewal_sub_end_month,
    turn_on_auto_renewal,
    COUNT(DISTINCT product_tier_name) OVER (PARTITION BY dim_subscription_id) AS sub_prod_count
  FROM {{ ref('mart_arr') }}
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

--Pulls existing Duo Trial lead cases
existing_duo_trial AS (
  SELECT 
  distinct
  dim_crm_account_id
  FROM {{ ref('prep_crm_case') }}
  WHERE record_type_id IN ('0128X000001pPRkQAM')
    AND subject = 'Duo Trial Started'
),

--Identifies which subscriptions have Duo on them
duo_on_sub AS (
  SELECT DISTINCT
    dim_crm_account_id,
    -- dim_subscription_id,
    -- product_rate_plan_name,
    -- subscription_end_month                                          AS auto_renewal_sub_end_month,
    -- turn_on_auto_renewal,
    COALESCE(CONTAINS(LOWER(product_rate_plan_name), 'duo'), FALSE) AS is_duo
  --arr_month,
  --max(arr_month) over(partition by dim_subscription_id) as latest_arr_month
  FROM mart_arr_all
  WHERE (arr_month = DATE_TRUNC('month', CURRENT_DATE()) OR arr_month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'))
    AND is_duo = TRUE
),


wk_sales_gds_cases AS (
  SELECT * FROM {{ ref('wk_sales_gds_cases') }}
),

wk_sales_gds_account_snapshots AS (
  SELECT 
  DISTINCT
  * FROM {{ ref('wk_sales_gds_account_snapshots') }}
  WHERE
    gds_account_flag
    AND snapshot_date = DATEADD('day', -1, CURRENT_DATE)
    AND is_deleted = FALSE
),

-- mart_arr as (
-- SELECT mart_arr.*
-- FROM "PROD"."RESTRICTED_SAFE_COMMON_MART_SALES"."MART_ARR_ALL" "MART_ARR"
-- INNER JOIN wk_sales_gds_account_snapshots 
-- on mart_arr.dim_crm_account_id = wk_sales_gds_account_snapshots.dim_crm_account_id
-- where mart_arr.arr_month = date_trunc('month',current_date)
-- ),

-- mart_product_usage_paid_user_metrics_monthly as (
--   SELECT MART_PRODUCT_USAGE_PAID_USER_METRICS_MONTHLY.*
-- FROM COMMON_MART_PRODUCT.MART_PRODUCT_USAGE_PAID_USER_METRICS_MONTHLY
-- INNER JOIN wk_sales_gds_account_snapshots 
-- on MART_PRODUCT_USAGE_PAID_USER_METRICS_MONTHLY.dim_crm_account_id = wk_sales_gds_account_snapshots.dim_crm_account_id
-- where MART_PRODUCT_USAGE_PAID_USER_METRICS_MONTHLY.is_latest_data
-- ),

wk_sales_gds_opportunities AS (
  SELECT * 
  FROM {{ ref('wk_sales_gds_opportunities') }}
),

smb_only_opportunities AS (
  SELECT * 
  FROM wk_sales_gds_opportunities
  WHERE
    fiscal_year >= 2024
    AND report_segment = 'SMB'
),

dim_subscription AS (
  SELECT * 
  FROM {{ ref('dim_subscription') }}
),

case_summary_prep AS (
  SELECT
    dim_crm_account_id,
    COUNT(DISTINCT CASE WHEN is_closed = FALSE AND subject NOT LIKE '%High Value%' THEN case_id END) AS open_nonhva_case_count,
    COUNT(DISTINCT CASE WHEN is_closed THEN case_id END)                                             AS closed_case_count,
    COUNT(DISTINCT CASE WHEN is_closed AND status = 'Closed: Resolved' THEN case_id END)             AS resolved_case_count,
    MAX(created_date)::DATE                                                                          AS most_recent_case_created_date,
    MAX(closed_date)::DATE                                                                           AS most_recent_case_closed_date,
    MAX(CASE WHEN is_closed AND status = 'Closed: Resolved' THEN closed_date END)::DATE              AS most_recent_case_resolved_date
  FROM wk_sales_gds_cases
  WHERE spam_checkbox = 0
    AND status NOT LIKE '%Duplicate%'
  GROUP BY 1
),

case_summary_trigger_prep AS (
  SELECT
    dim_crm_account_id,
    CASE WHEN case_trigger LIKE '%2024%'
        THEN
          TRIM(LEFT(case_trigger, CHARINDEX('2024', case_trigger) - 1))
      ELSE COALESCE(case_trigger, subject, 'No subject OR trigger') END
      AS trigger_clean,
    COUNT(DISTINCT CASE WHEN is_closed = FALSE THEN case_id END) OVER (PARTITION BY dim_crm_account_id, trigger_clean)                                        AS open_case_count,
    COUNT(DISTINCT CASE WHEN is_closed THEN case_id END) OVER (PARTITION BY dim_crm_account_id, trigger_clean)                                                AS closed_case_count,
    COUNT(DISTINCT CASE WHEN is_closed AND status = 'Closed: Resolved' THEN case_id END) OVER (PARTITION BY dim_crm_account_id, trigger_clean)                AS resolved_case_count,
    MAX(created_date) OVER (PARTITION BY dim_crm_account_id, trigger_clean)::DATE                                                                             AS most_recent_case_created_date,
    MAX(closed_date) OVER (PARTITION BY dim_crm_account_id, trigger_clean)::DATE                                                                              AS most_recent_case_closed_date,
    MAX(CASE WHEN is_closed AND status = 'Closed: Resolved' THEN closed_date END) OVER (PARTITION BY dim_crm_account_id, trigger_clean)::DATE                 AS most_recent_case_resolved_date,
    FIRST_VALUE(trigger_clean) OVER (PARTITION BY dim_crm_account_id ORDER BY created_date DESC)                                                              AS account_most_recent_created_trigger,
    FIRST_VALUE(CASE WHEN is_closed AND status = 'Closed: Resolved' THEN trigger_clean END) OVER (PARTITION BY dim_crm_account_id ORDER BY created_date DESC) AS account_most_recent_resolved_trigger

  FROM wk_sales_gds_cases
  WHERE spam_checkbox = 0
    AND status NOT LIKE '%Duplicate%'
    AND dim_crm_account_id IS NOT NULL
),

case_trigger_summary AS (
  SELECT DISTINCT
    dim_crm_account_id,
    account_most_recent_created_trigger,
    account_most_recent_resolved_trigger
  FROM case_summary_trigger_prep
),

--Initial portion of the query gets the CASE disposition of each GDS account, for later filtering
account_case_summary AS (
  SELECT
    wk_sales_gds_account_snapshots.*,
    case_summary_prep.* EXCLUDE (dim_crm_account_id),
    case_trigger_summary.* EXCLUDE (dim_crm_account_id)
  FROM wk_sales_gds_account_snapshots
  LEFT JOIN case_summary_prep
    ON wk_sales_gds_account_snapshots.dim_crm_account_id = case_summary_prep.dim_crm_account_id
  LEFT JOIN case_trigger_summary
    ON wk_sales_gds_account_snapshots.dim_crm_account_id = case_trigger_summary.dim_crm_account_id
),

account_case_flags AS (
  SELECT
    DATEADD('day', -180, CURRENT_DATE)                                       AS current_date_180_days,
    DATEADD('day', -90, CURRENT_DATE)                                        AS current_date_90_days,
    DATEADD('day', -60, CURRENT_DATE)                                        AS current_date_60_days,
    DATEADD('day', -30, CURRENT_DATE)                                        AS current_date_30_days,

    account_case_summary.*,
    COALESCE(high_value_case_id IS NOT NULL, FALSE)                          AS has_high_value_case,
    COALESCE(most_recent_case_resolved_date >= current_date_180_days, FALSE) AS has_resolved_case_last_180,
    COALESCE(most_recent_case_resolved_date >= current_date_90_days, FALSE)  AS has_resolved_case_last_90,
    COALESCE(most_recent_case_resolved_date >= current_date_60_days, FALSE)  AS has_resolved_case_last_60,
    COALESCE(most_recent_case_resolved_date >= current_date_30_days, FALSE)  AS has_resolved_case_last_30,
    COALESCE(open_nonhva_case_count > 0, FALSE)                              AS has_open_non_highvalue_cases

  FROM account_case_summary

),

opportunity_renewal_prep AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY dim_crm_account_id ORDER BY close_date ASC) AS close_date_rank_order,
    RANK() OVER (PARTITION BY dim_crm_account_id, close_date ORDER BY arr_basis_for_clari DESC) AS close_date_multiple_narr_order,
    RANK() OVER (PARTITION BY dim_crm_account_id, close_date,arr_basis_for_clari ORDER BY created_date DESC) AS
duplicate_renewal_order
  FROM wk_sales_gds_opportunities
  WHERE subscription_type = 'Renewal'
    AND is_closed = FALSE
    AND close_date > CURRENT_DATE
    AND arr_basis_for_clari > 0
),

opportunity_renewal_output AS (
  SELECT DISTINCT *
  FROM opportunity_renewal_prep
  WHERE close_date_rank_order = 1
  and close_date_multiple_narr_order = 1
  and duplicate_renewal_order = 1
),

-- opportunity_growth_prep AS (
--   SELECT
--     *,
--     RANK() OVER (PARTITION BY dim_crm_account_id ORDER BY close_date DESC)          AS close_date_rank_order,
--     RANK() OVER (PARTITION BY dim_crm_account_id, close_date ORDER BY net_arr DESC) AS close_date_multiple_narr_order
--   FROM wk_sales_gds_opportunities
--   WHERE subscription_type = 'Add-On Business'
--     AND is_closed
--     AND is_won
--     AND net_arr > 0
-- ),

-- opportunity_growth_output AS (
--   SELECT DISTINCT *
--   FROM opportunity_growth_prep
--   WHERE close_date_rank_order = 1
--     AND close_date_multiple_narr_order = 1
-- ),

subscription_prep AS (
  SELECT
    *,
    MAX(subscription_version) OVER (PARTITION BY subscription_name ORDER BY subscription_version DESC)                                                                            AS latest_version,
    MAX(CASE WHEN dim_crm_opportunity_id_current_open_renewal IS NOT NULL THEN subscription_version END) OVER (PARTITION BY subscription_name ORDER BY subscription_version DESC) AS latest_version_with_open_renewal
  FROM dim_subscription
  QUALIFY (
    dim_crm_opportunity_id_current_open_renewal IS NOT NULL AND subscription_version = latest_version_with_open_renewal
  )
  OR
  subscription_version = latest_version
),

--SELECT * FROM opportunity_renewal_output ORDER BY 2
final_disposition AS (
  SELECT
    account_case_flags.* EXCLUDE(created_by, updated_by,dbt_created_at,dbt_updated_at,model_created_date,model_updated_date),
    opportunity_renewal_output.dim_crm_opportunity_id                AS dim_crm_opportunity_id_next_renewal,
    opportunity_renewal_output.close_date                            AS close_date_next_renewal,
    opportunity_renewal_output.arr_basis_for_clari                   AS arr_basis_next_renewal,
    -- opportunity_growth_output.dim_crm_opportunity_id                 AS dim_crm_opportunity_id_most_recent_growth,
    -- opportunity_growth_output.close_date                             AS close_date_most_recent_growth,
    -- opportunity_growth_output.net_arr                                AS net_arr_most_recent_growth,
    subscription_prep.dim_subscription_id                            AS sub_subscription_id,
    subscription_prep.dim_subscription_id_original                   AS sub_subscription_id_original,
    subscription_prep.subscription_status,
    subscription_prep.term_start_date                                AS current_subscription_start_date,
    subscription_prep.term_end_date                                  AS current_subscription_end_date,
    DATEDIFF('day', subscription_prep.term_start_date, CURRENT_DATE) AS days_into_current_subscription,
    subscription_prep.turn_on_auto_renewal,
    subscription_prep.turn_on_seat_reconciliation,
    CASE WHEN account_case_flags.crm_account_owner = 'EMEA SMB Sales' OR account_case_flags.crm_account_owner = 'APAC SMB Sales' THEN 'EMEA'
      ELSE 'AMER'
    END                                                              AS team_name_for_join,
    billing_accounts.po_required,
    COALESCE(
      auto_renew_will_fail.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                                AS auto_renew_will_fail_mult_products,
    COALESCE(
      duo_on_sub.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                                AS duo_on_sub_flag,
    COALESCE(
      existing_duo_trial.dim_crm_account_id IS NOT NULL,
      FALSE
    )                                                                AS existing_duo_trial_flag,
    COALESCE(
      qsr_failed_last_75.qsr_failed_last_75 = TRUE,
      FALSE
    )                                                         AS qsr_failed_last_75
  FROM account_case_flags
  LEFT JOIN opportunity_renewal_output
    ON account_case_flags.dim_crm_account_id = opportunity_renewal_output.dim_crm_account_id
  -- LEFT JOIN opportunity_growth_output
  --   ON account_case_flags.dim_crm_account_id = opportunity_growth_output.dim_crm_account_id
  LEFT JOIN subscription_prep
    ON opportunity_renewal_output.dim_crm_opportunity_id = subscription_prep.dim_crm_opportunity_id_current_open_renewal
      -- OR (opportunity_growth_output.dim_crm_opportunity_id = subscription_prep.dim_crm_opportunity_id AND subscription_prep.dim_crm_opportunity_id_current_open_renewal IS NULL)
  LEFT JOIN billing_accounts
    ON account_case_flags.dim_crm_account_id = billing_accounts.dim_crm_account_id
  LEFT JOIN qsr_failed_last_75
    ON account_case_flags.dim_crm_account_id = qsr_failed_last_75.dim_crm_account_id
  LEFT JOIN auto_renew_will_fail
    ON account_case_flags.dim_crm_account_id = auto_renew_will_fail.dim_crm_account_id
      AND subscription_prep.dim_subscription_id = auto_renew_will_fail.dim_subscription_id
  LEFT JOIN existing_duo_trial
    ON account_case_flags.dim_crm_account_id = existing_duo_trial.dim_crm_account_id
  LEFT JOIN duo_on_sub
    ON account_case_flags.dim_crm_account_id = duo_on_sub.dim_crm_account_id
--      AND subscription_prep.dim_subscription_id = duo_on_sub.dim_subscription_id
)

{{ dbt_audit(
    cte_ref="final_disposition",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-10-29",
    updated_date="2024-10-29"
) }}
