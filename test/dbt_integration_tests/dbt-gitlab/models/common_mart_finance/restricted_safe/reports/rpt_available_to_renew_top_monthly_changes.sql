{{ simple_cte([
    ('mart_atr_snapshot', 'mart_available_to_renew_snapshot_model'),
    ('dim_date', 'dim_date')
]) }},

atr_daily AS (

  SELECT
    dim_date.first_day_of_month                                                  AS snapshot_month,
    mart_atr_snapshot.parent_crm_account_name,
    mart_atr_snapshot.snapshot_date,
    mart_atr_snapshot.subscription_name,
    COALESCE(multi_year_booking_subscription_end_month, bookings_term_end_month) AS subscription_final_month,
    mart_atr_snapshot.renewal_month,
    mart_atr_snapshot.fiscal_quarter_name_fy,
    mart_atr_snapshot.dim_subscription_id,
    mart_atr_snapshot.dim_crm_opportunity_id,
    mart_atr_snapshot.product_tier_name,
    mart_atr_snapshot.subscription_start_month,
    mart_atr_snapshot.subscription_end_month,
    mart_atr_snapshot.multi_year_booking_subscription_end_month,
    mart_atr_snapshot.is_available_to_renew,
    SUM(arr)::NUMBER                                                             AS total_arr,
    MAX(mart_atr_snapshot.fiscal_year)                                           AS max_year
  FROM mart_atr_snapshot
  INNER JOIN dim_date ON mart_atr_snapshot.snapshot_date = dim_date.date_day
  {{ dbt_utils.group_by(n=14) }}

),

atr_comparison AS (

  SELECT
    *,
    LAG(is_available_to_renew) OVER (PARTITION BY subscription_name ORDER BY snapshot_date) AS is_available_to_renew_previous_day,
    CASE WHEN is_available_to_renew = TRUE
        AND LAG(is_available_to_renew)
        OVER (PARTITION BY subscription_name ORDER BY snapshot_date) = FALSE
        THEN 'Became available to renew'
      WHEN is_available_to_renew = FALSE
        AND LAG(is_available_to_renew)
        OVER (PARTITION BY subscription_name ORDER BY snapshot_date) = TRUE
        THEN 'Dropped as available to renew'
      WHEN LAG(is_available_to_renew)
        OVER (PARTITION BY subscription_name ORDER BY snapshot_date) IS NULL
        THEN 'New subscription'
      WHEN subscription_final_month = snapshot_month
        THEN 'Subscription ends this month'
      WHEN is_available_to_renew = FALSE 
      AND dense_rank() OVER (PARTITION BY snapshot_month ORDER BY total_arr DESC) <= 10 
        THEN 'Top available to renew excluded deal'
      ELSE 'No change'
    END                                                                                     AS atr_change_flag
  FROM atr_daily
  WHERE max_year = DATE_PART(YEAR, CURRENT_DATE())

),

monthly_agg AS (

  SELECT DISTINCT
    snapshot_month,
    parent_crm_account_name,
    subscription_name,
    renewal_month,
    fiscal_quarter_name_fy,
    dim_subscription_id,
    dim_crm_opportunity_id,
    product_tier_name,
    subscription_start_month,
    subscription_end_month,
    multi_year_booking_subscription_end_month,
    total_arr,
    atr_change_flag
  FROM atr_comparison

),

final AS (

  SELECT 
    snapshot_month,
    parent_crm_account_name,
    subscription_name,
    renewal_month,
    fiscal_quarter_name_fy,
    dim_subscription_id,
    dim_crm_opportunity_id,
    product_tier_name,
    subscription_start_month,
    subscription_end_month,
    multi_year_booking_subscription_end_month,
    total_arr,
    atr_change_flag
  FROM monthly_agg
  WHERE atr_change_flag != 'No change'
    AND snapshot_month != '2021-12-01' -- exclude first month otherwise everyone is a new subscription
  QUALIFY RANK() OVER (
      PARTITION BY snapshot_month, atr_change_flag
      ORDER BY total_arr DESC) <= 10

)

SELECT *
FROM final
