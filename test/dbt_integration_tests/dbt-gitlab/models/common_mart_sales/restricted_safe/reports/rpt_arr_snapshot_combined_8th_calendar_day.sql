{{ simple_cte([
    ('driveload_financial_metrics_program_phase_1_source','driveload_financial_metrics_program_phase_1_source'),
    ('dim_date','dim_date'),
    ('mart_arr_snapshot_model','mart_arr_snapshot_model'),
    ('mart_arr','mart_arr_snapshot_bottom_up'),
    ('mart_arr_current', 'mart_arr')
]) }}

, dim_date_actual AS (
  
    -- This CTE controls which months will be added from `mart_arr_current` to this table
    -- If the last date in the snapshot table is below 7 we have to add two dates. The max month of the snapshot and the month before it
    -- Example: if last date snapshot = 2023-08-07, then we need to add 2023-07-01 (as we are not yet on 2023-08-08) and also 2023-08-01 (current_month) to the table
    SELECT
      first_day_of_month,
      snapshot_date_fpa,
      date_actual,
      (SELECT MAX(snapshot_date) FROM mart_arr_snapshot_model) AS max_snapshot_date
    FROM dim_date
    WHERE CASE
        WHEN DAY(max_snapshot_date) <= 7
          THEN date_actual = max_snapshot_date
            OR date_actual = DATEADD('month', -1, max_snapshot_date)
        ELSE date_actual = max_snapshot_date
      END

), mart_arr_snapshot_model_combined AS (

    SELECT
      TRUE AS is_arr_month_finalized,
      snapshot_date,
      arr_month,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      -- Coalescing these two values since sales segment has been changed in SFDC but finance reporting needs to keep using the legacy value
      -- Background issue: https://gitlab.com/gitlab-data/analytics/-/issues/20395
      COALESCE(parent_crm_account_sales_segment_legacy, parent_crm_account_sales_segment)       AS parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_tier_name,
      product_rate_plan_name,
      product_rate_plan_charge_name,
      product_deployment_type,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      dim_charge_id,
      is_licensed_user,
      parent_crm_account_employee_count_band,
      is_jihu_account
    FROM mart_arr_snapshot_model

    UNION ALL

    SELECT
      FALSE AS is_arr_month_finalized,
      dim_date_actual.snapshot_date_fpa                                                         AS snapshot_date,
      arr_month,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      NULL                                                                                      AS parent_crm_account_billing_country,
      -- Sales segment has been changed in SFDC but finance reporting needs to keep using the legacy value
      -- Background issue: https://gitlab.com/gitlab-data/analytics/-/issues/20395
      parent_crm_account_sales_segment_legacy                                                   AS parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      NULL                                                                                      AS parent_crm_account_owner_team,
      NULL                                                                                      AS parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_tier_name,
      product_rate_plan_name,
      product_rate_plan_charge_name,
      product_deployment_type,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      dim_charge_id,
      is_licensed_user,
      NULL                                                                                      AS parent_crm_account_employee_count_band,
      is_jihu_account
    FROM mart_arr_current
    INNER JOIN dim_date_actual
      ON mart_arr_current.arr_month = dim_date_actual.first_day_of_month
      
), phase_one AS (

    SELECT
      TRUE                                                                                         AS is_arr_month_finalized,
      driveload_financial_metrics_program_phase_1_source.arr_month,
      driveload_financial_metrics_program_phase_1_source.fiscal_quarter_name_fy,
      driveload_financial_metrics_program_phase_1_source.fiscal_year,
      driveload_financial_metrics_program_phase_1_source.subscription_start_month,
      driveload_financial_metrics_program_phase_1_source.subscription_end_month,
      driveload_financial_metrics_program_phase_1_source.zuora_account_id                          AS dim_billing_account_name,
      driveload_financial_metrics_program_phase_1_source.zuora_sold_to_country                     AS sold_to_country,
      driveload_financial_metrics_program_phase_1_source.zuora_account_name                        AS billing_account_name,
      driveload_financial_metrics_program_phase_1_source.zuora_account_number                      AS billing_account_number,
      driveload_financial_metrics_program_phase_1_source.dim_crm_account_id,
      driveload_financial_metrics_program_phase_1_source.dim_parent_crm_account_id,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_name,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_billing_country,
      CASE
       WHEN driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment IS NULL THEN 'SMB'
       WHEN driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment = 'Pubsec' THEN 'PubSec'
       ELSE driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment
      END                                                                                          AS parent_crm_account_sales_segment,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_industry,
      NULL                                                                                         AS parent_crm_account_geo,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_owner_team,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_territory,
      NULL                                                                                         AS dim_subscription_id,
      driveload_financial_metrics_program_phase_1_source.subscription_name,
      driveload_financial_metrics_program_phase_1_source.subscription_status,
      driveload_financial_metrics_program_phase_1_source.subscription_sales_type,
      driveload_financial_metrics_program_phase_1_source.product_name,
      NULL                                                                                         AS product_rate_plan_name,
      NULL                                                                                         AS product_rate_plan_charge_name,
      NULL                                                                                         AS product_deployment_type,
      driveload_financial_metrics_program_phase_1_source.product_category                          AS product_tier_name,
      CASE
        WHEN  driveload_financial_metrics_program_phase_1_source.delivery = 'Others' THEN 'SaaS'
        ELSE  driveload_financial_metrics_program_phase_1_source.delivery
      END                                                                                          AS product_delivery_type,
      NULL                                                                                         AS product_ranking,
      driveload_financial_metrics_program_phase_1_source.service_type,
      driveload_financial_metrics_program_phase_1_source.unit_of_measure,
      driveload_financial_metrics_program_phase_1_source.mrr,
      driveload_financial_metrics_program_phase_1_source.arr,
      driveload_financial_metrics_program_phase_1_source.quantity,
      NULL                                                                                         AS is_arpu,
      NULL                                                                                         AS dim_charge_id,
      /*
      The is_licensed_user is not available in the driveload file. We can use the product_tier_name to fill in the historical data.
      This is the same logic found in prep_product_detail.
      */
      CASE
        WHEN product_tier_name = 'Storage' THEN FALSE
        WHEN product_tier_name = 'Other' THEN FALSE
        ELSE TRUE
      END                                                                                          AS is_licensed_user,
      driveload_financial_metrics_program_phase_1_source.parent_account_cohort_month,
      driveload_financial_metrics_program_phase_1_source.months_since_parent_account_cohort_start,
      driveload_financial_metrics_program_phase_1_source.parent_crm_account_employee_count_band
    FROM driveload_financial_metrics_program_phase_1_source
    WHERE arr_month <= '2021-06-01'

), snapshot_dates AS (
    --Use the 8th calendar day to snapshot ARR, Licensed Users, and Customer Count Metrics
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_fpa
    FROM dim_date
    ORDER BY 1 DESC

), parent_cohort_month_snapshot AS (

    SELECT
      dim_parent_crm_account_id,
      MIN(arr_month)                                                                             AS parent_account_cohort_month
    FROM mart_arr_snapshot_model_combined
    {{ dbt_utils.group_by(n=1) }}

), snapshot_model AS (

    SELECT
      mart_arr_snapshot_model_combined.is_arr_month_finalized,
      mart_arr_snapshot_model_combined.arr_month,
      mart_arr_snapshot_model_combined.fiscal_quarter_name_fy,
      mart_arr_snapshot_model_combined.fiscal_year,
      mart_arr_snapshot_model_combined.subscription_start_month,
      mart_arr_snapshot_model_combined.subscription_end_month,
      mart_arr_snapshot_model_combined.dim_billing_account_id,
      mart_arr_snapshot_model_combined.sold_to_country,
      mart_arr_snapshot_model_combined.billing_account_name,
      mart_arr_snapshot_model_combined.billing_account_number,
      mart_arr_snapshot_model_combined.dim_crm_account_id,
      mart_arr_snapshot_model_combined.dim_parent_crm_account_id,
      mart_arr_snapshot_model_combined.parent_crm_account_name,
      mart_arr_snapshot_model_combined.parent_crm_account_billing_country,
      CASE
       WHEN mart_arr_snapshot_model_combined.parent_crm_account_sales_segment IS NULL THEN 'SMB'
       WHEN mart_arr_snapshot_model_combined.parent_crm_account_sales_segment = 'Pubsec' THEN 'PubSec'
       ELSE mart_arr_snapshot_model_combined.parent_crm_account_sales_segment
      END                                                                                       AS parent_crm_account_sales_segment,
      mart_arr_snapshot_model_combined.parent_crm_account_industry,
      mart_arr_snapshot_model_combined.parent_crm_account_geo,
      mart_arr_snapshot_model_combined.parent_crm_account_owner_team,
      mart_arr_snapshot_model_combined.parent_crm_account_sales_territory                                      AS parent_crm_account_sales_territory,
      mart_arr_snapshot_model_combined.dim_subscription_id,
      mart_arr_snapshot_model_combined.subscription_name,
      mart_arr_snapshot_model_combined.subscription_status,
      mart_arr_snapshot_model_combined.subscription_sales_type,
      CASE
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Ultimate' THEN 'Ultimate'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Dedicated - Ultimate'    THEN 'Ultimate'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Premium'  THEN 'Premium'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Starter'  THEN 'Bronze/Starter'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Ultimate'         THEN 'Ultimate'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Premium'          THEN 'Premium'
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Bronze'           THEN 'Bronze/Starter'
        ELSE mart_arr_snapshot_model_combined.product_tier_name
      END                                                                                       AS product_name,
      mart_arr_snapshot_model_combined.product_rate_plan_name,
      mart_arr_snapshot_model_combined.product_rate_plan_charge_name,
      mart_arr_snapshot_model_combined.product_deployment_type,
      mart_arr_snapshot_model_combined.product_tier_name,
      CASE
        WHEN  mart_arr_snapshot_model_combined.product_delivery_type = 'Others' THEN 'SaaS'
        ELSE  mart_arr_snapshot_model_combined.product_delivery_type
      END                                                                                       AS  product_delivery_type,
      mart_arr_snapshot_model_combined.product_ranking,
      mart_arr_snapshot_model_combined.service_type,
      mart_arr_snapshot_model_combined.unit_of_measure,
      mart_arr_snapshot_model_combined.mrr,
      mart_arr_snapshot_model_combined.arr,
      mart_arr_snapshot_model_combined.quantity,
      mart_arr_snapshot_model_combined.is_arpu,
      mart_arr_snapshot_model_combined.dim_charge_id,
      /*
      The is_licensed_user flag was added in 2022-08-01 to the mart_arr and mart_arr_snapshot_model_combined models. There is no historical data for the is_licensed_user
      flag prior to 2022-08-01. We can use the product_tier_name to fill in the historical data. This is the same logic found in prep_product_detail.
      */
      CASE
        WHEN mart_arr_snapshot_model_combined.is_licensed_user IS NOT NULL
          THEN mart_arr_snapshot_model_combined.is_licensed_user
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Storage'
          THEN FALSE
        WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Other'
          THEN FALSE
        ELSE TRUE
      END                                                                                       AS is_licensed_user,
      parent_cohort_month_snapshot.parent_account_cohort_month                                  AS parent_account_cohort_month,
      DATEDIFF(month, parent_cohort_month_snapshot.parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
      mart_arr_snapshot_model_combined.parent_crm_account_employee_count_band
    FROM mart_arr_snapshot_model_combined
    INNER JOIN snapshot_dates
      ON mart_arr_snapshot_model_combined.arr_month = snapshot_dates.first_day_of_month
      AND mart_arr_snapshot_model_combined.snapshot_date = snapshot_dates.snapshot_date_fpa
    --calculate parent cohort month based on correct cohort logic
    LEFT JOIN parent_cohort_month_snapshot
      ON mart_arr_snapshot_model_combined.dim_parent_crm_account_id = parent_cohort_month_snapshot.dim_parent_crm_account_id
    WHERE mart_arr_snapshot_model_combined.is_jihu_account != 'TRUE'
      AND mart_arr_snapshot_model_combined.arr_month >= '2021-07-01'
    ORDER BY 1 DESC

), combined AS (

    SELECT *
    FROM snapshot_model

    UNION ALL

    SELECT *
    FROM phase_one

), parent_arr AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                   AS arr
    FROM combined
    GROUP BY 1,2

), parent_arr_band_calc AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                        AS arr_band_calc
    FROM parent_arr

), edu_subscriptions AS (

    /*
    The is_arpu flag was added in 2022-08-01 to the mart_arr and mart_arr_snapshot_model_combined models. There is no historical data for the is_arpu
    flag prior to 2022-08-01. Moreover, the required product_rate_plan_name is not in the driveload financial metrics file to build out the flag.
    Therefore, we can search for the subscriptions themselves to flag the EDU subscriptions and use the product_tier_name to flag the storage
    related charges to fill in the historical data.
    */
    SELECT DISTINCT
      subscription_name
    FROM mart_arr
    WHERE product_rate_plan_name LIKE '%EDU%'
      AND arr_month <= '2022-07-01'

), final AS (
    --Snap in arr_band_calc based on correct logic. Some historical in mart_arr_snapshot_model_combined do not have the arr_band_calc.
    SELECT
      combined.arr_month,
      combined.is_arr_month_finalized,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      combined.dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      combined.dim_crm_account_id,
      combined.dim_parent_crm_account_id,
      combined.parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      combined.dim_subscription_id,
      combined.subscription_name,
      subscription_status,
      subscription_sales_type,
      product_name,
      CASE
        WHEN product_name NOT IN ('Ultimate', 'Premium', 'Bronze/Starter') 
          THEN 'All Others'
        ELSE product_name
      END                                                                                AS product_name_grouped,
      product_rate_plan_name,
      product_rate_plan_charge_name,
      product_deployment_type,
      product_tier_name,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      --This logic fills in the missing data and uses the core logic found in prep_product_detail to make the is_arpu flag.
      CASE
        WHEN combined.is_arpu IS NOT NULL
          THEN is_arpu
        WHEN combined.product_tier_name = 'Storage'
          THEN FALSE
        WHEN combined.product_rate_plan_name LIKE '%EDU%'
          THEN FALSE
        WHEN edu_subscriptions.subscription_name IS NOT NULL
          THEN FALSE
        ELSE TRUE
      END                                                                               AS is_arpu,
      dim_charge_id,
      is_licensed_user,
      parent_account_cohort_month,
      months_since_parent_account_cohort_start,
      COALESCE(parent_arr_band_calc.arr_band_calc, 'Missing crm_account_id')            AS arr_band_calc,
      parent_crm_account_employee_count_band
    FROM combined
    LEFT JOIN parent_arr_band_calc
      ON combined.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id
      AND combined.arr_month = parent_arr_band_calc.arr_month
    LEFT JOIN edu_subscriptions
      ON combined.subscription_name = edu_subscriptions.subscription_name
    WHERE combined.arr_month < '2024-03-01' -- month from when we switched from 8th to 5th day snapshot

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@chrissharp",
    created_date="2021-08-16",
    updated_date="2024-12-18"
) }}
