{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly'),
    ('rpt_ping_subscriptions_reported_counts_monthly', 'rpt_ping_subscriptions_reported_counts_monthly'),
    ('rpt_ping_subscriptions_on_versions_counts_monthly', 'rpt_ping_subscriptions_on_versions_counts_monthly'),
    ('latest_subscriptions', 'rpt_ping_latest_subscriptions_monthly'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])
}},

-- Get value from latest_susbcriptions

arr_joined AS (

  SELECT
    mart_ping_instance_metric_monthly.ping_created_date_month       AS ping_created_date_month,
    mart_ping_instance_metric_monthly.ping_delivery_type            AS ping_delivery_type,
    mart_ping_instance_metric_monthly.ping_deployment_type          AS ping_deployment_type,
    mart_ping_instance_metric_monthly.metrics_path                  AS metrics_path,
    mart_ping_instance_metric_monthly.ping_edition                  AS ping_edition,
    mart_ping_instance_metric_monthly.stage_name                    AS stage_name,
    mart_ping_instance_metric_monthly.section_name                  AS section_name,
    mart_ping_instance_metric_monthly.group_name                    AS group_name,
    mart_ping_instance_metric_monthly.is_smau                       AS is_smau,
    mart_ping_instance_metric_monthly.is_gmau                       AS is_gmau,
    mart_ping_instance_metric_monthly.is_paid_gmau                  AS is_paid_gmau,
    mart_ping_instance_metric_monthly.is_umau                       AS is_umau,
    mart_ping_instance_metric_monthly.latest_subscription_id        AS latest_subscription_id,
    latest_subscriptions.licensed_user_count                        AS licensed_user_count
  FROM mart_ping_instance_metric_monthly
  INNER JOIN latest_subscriptions
    ON mart_ping_instance_metric_monthly.latest_subscription_id = latest_subscriptions.latest_subscription_id
      AND mart_ping_instance_metric_monthly.ping_created_date_month = latest_subscriptions.ping_created_date_month
      AND mart_ping_instance_metric_monthly.ping_deployment_type = latest_subscriptions.ping_deployment_type
  WHERE mart_ping_instance_metric_monthly.time_frame IN ('28d', 'all')
    AND mart_ping_instance_metric_monthly.ping_deployment_type IN ('Self-Managed', 'Dedicated')
  {{ dbt_utils.group_by(n=14) }}

),

-- Get actual count of subs/users for a given month/metric

reported_actuals AS (

  SELECT
    ping_created_date_month                                         AS ping_created_date_month,
    ping_delivery_type                                              AS ping_delivery_type,
    ping_deployment_type                                            AS ping_deployment_type,
    metrics_path                                                    AS metrics_path,
    ping_edition                                                    AS ping_edition,
    stage_name                                                      AS stage_name,
    section_name                                                    AS section_name,
    group_name                                                      AS group_name,
    is_smau                                                         AS is_smau,
    is_gmau                                                         AS is_gmau,
    is_paid_gmau                                                    AS is_paid_gmau,
    is_umau                                                         AS is_umau,
    COUNT(DISTINCT latest_subscription_id)                          AS subscription_count,
    SUM(licensed_user_count)                                        AS seat_count
  FROM arr_joined
  {{ dbt_utils.group_by(n=12) }}

),

-- Join actuals to number of possible subs/users

joined_counts AS (

  SELECT
    reported_actuals.ping_created_date_month                                                                     AS ping_created_date_month,
    reported_actuals.ping_delivery_type                                                                          AS ping_delivery_type,
    reported_actuals.ping_deployment_type                                                                        AS ping_deployment_type,
    reported_actuals.metrics_path                                                                                AS metrics_path,
    rpt_ping_subscriptions_reported_counts_monthly.ping_edition                                                  AS ping_edition,
    reported_actuals.stage_name                                                                                  AS stage_name,
    reported_actuals.section_name                                                                                AS section_name,
    reported_actuals.group_name                                                                                  AS group_name,
    reported_actuals.is_smau                                                                                     AS is_smau,
    reported_actuals.is_gmau                                                                                     AS is_gmau,
    reported_actuals.is_paid_gmau                                                                                AS is_paid_gmau,
    reported_actuals.is_umau                                                                                     AS is_umau,
    rpt_ping_subscriptions_on_versions_counts_monthly.total_subscription_count                                   AS reported_subscription_count, -- on version with metric
    rpt_ping_subscriptions_on_versions_counts_monthly.total_licensed_users                                       AS reported_seat_count, -- on version with metric
    rpt_ping_subscriptions_reported_counts_monthly.total_licensed_users                                          AS total_licensed_users,  -- could have reported (total seats on latest subs)
    rpt_ping_subscriptions_reported_counts_monthly.total_subscription_count                                      AS total_subscription_count, -- could have reported (total latest subs)
    rpt_ping_subscriptions_reported_counts_monthly.total_subscription_count - reported_subscription_count        AS not_reporting_subscription_count, -- not on version with metric
    rpt_ping_subscriptions_reported_counts_monthly.total_licensed_users - reported_seat_count                    AS not_reporting_seat_count -- not on version with metric
  FROM reported_actuals
  INNER JOIN rpt_ping_subscriptions_on_versions_counts_monthly --model with subscriptions and seats on version
    ON reported_actuals.ping_created_date_month = rpt_ping_subscriptions_on_versions_counts_monthly.ping_created_date_month
    AND reported_actuals.metrics_path = rpt_ping_subscriptions_on_versions_counts_monthly.metrics_path
    AND reported_actuals.ping_deployment_type = rpt_ping_subscriptions_on_versions_counts_monthly.ping_deployment_type
  INNER JOIN rpt_ping_subscriptions_reported_counts_monthly --model with overall total subscriptions and seats
    ON reported_actuals.ping_created_date_month = rpt_ping_subscriptions_reported_counts_monthly.ping_created_date_month
      AND reported_actuals.metrics_path = rpt_ping_subscriptions_reported_counts_monthly.metrics_path
      AND rpt_ping_subscriptions_on_versions_counts_monthly.ping_edition = rpt_ping_subscriptions_reported_counts_monthly.ping_edition
      AND reported_actuals.ping_deployment_type = rpt_ping_subscriptions_reported_counts_monthly.ping_deployment_type

),

-- Split subs and seats then union

unioned_counts AS (

  SELECT
    ping_created_date_month                                         AS ping_created_date_month,
    ping_delivery_type                                              AS ping_delivery_type,
    ping_deployment_type                                            AS ping_deployment_type,
    metrics_path                                                    AS metrics_path,
    ping_edition                                                    AS ping_edition,
    stage_name                                                      AS stage_name,
    section_name                                                    AS section_name,
    group_name                                                      AS group_name,
    is_smau                                                         AS is_smau,
    is_gmau                                                         AS is_gmau,
    is_paid_gmau                                                    AS is_paid_gmau,
    is_umau                                                         AS is_umau,
    reported_subscription_count                                     AS reporting_count,
    not_reporting_subscription_count                                AS not_reporting_count,
    total_subscription_count                                        AS total_count,
    'metric/version check - subscription based estimation'          AS estimation_grain
  FROM joined_counts

  UNION ALL

  SELECT
    ping_created_date_month                                         AS ping_created_date_month,
    ping_delivery_type                                              AS ping_delivery_type,
    ping_deployment_type                                            AS ping_deployment_type,
    metrics_path                                                    AS metrics_path,
    ping_edition                                                    AS ping_edition,
    stage_name                                                      AS stage_name,
    section_name                                                    AS section_name,
    group_name                                                      AS group_name,
    is_smau                                                         AS is_smau,
    is_gmau                                                         AS is_gmau,
    is_paid_gmau                                                    AS is_paid_gmau,
    is_umau                                                         AS is_umau,
    reported_seat_count                                             AS reporting_count,
    not_reporting_seat_count                                        AS not_reporting_count,
    total_licensed_users                                            AS total_count,
    'metric/version check - seat based estimation'                  AS estimation_grain
  FROM joined_counts

),

-- Create PK and use macro for percent_reporting

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['ping_created_date_month', 'metrics_path', 'ping_edition','estimation_grain', 'ping_deployment_type']) }} AS ping_subscriptions_on_versions_estimate_factors_monthly_id,
    *,
    {{ pct_w_counters('reporting_count', 'not_reporting_count') }}                                                AS percent_reporting
  FROM unioned_counts

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@jpeguero",
    created_date="2022-04-07",
    updated_date="2023-06-26"
) }}
