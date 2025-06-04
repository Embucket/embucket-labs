{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
  ('latest_subscriptions', 'rpt_ping_latest_subscriptions_monthly'),
  ('mart_ping_instance_metric', 'mart_ping_instance_metric')
    ])

}}

-- Determine monthly sub and user count

, dedupe_latest_subscriptions AS (

  SELECT
    ping_created_date_month                                           AS ping_created_date_month,
    --ping_edition                                                      AS ping_edition,
    latest_subscription_id                                            AS latest_subscription_id,
    ping_delivery_type                                                AS ping_delivery_type,
    ping_deployment_type                                              AS ping_deployment_type,
    licensed_user_count                                               AS licensed_user_count
  FROM latest_subscriptions
    {{ dbt_utils.group_by(n=5)}}

), subscription_info AS (

  SELECT
    ping_created_date_month                                           AS ping_created_date_month,
    --ping_edition                                                      AS ping_edition,
    ping_delivery_type                                                AS ping_delivery_type,
    ping_deployment_type                                              AS ping_deployment_type,
    1                                                                 AS key,
    COUNT(DISTINCT latest_subscription_id)                            AS total_subscription_count,
    SUM(licensed_user_count)                                          AS total_licensed_users
  FROM dedupe_latest_subscriptions
      {{ dbt_utils.group_by(n=4)}}

), metrics AS (

    SELECT --grab all metrics and editions reported on a given month
      ping_created_date_month,
      metrics_path,
      ping_edition,
      1 AS key
    FROM mart_ping_instance_metric
    {{ dbt_utils.group_by(n=4)}}

-- Join to get combo of all possible subscriptions and the metrics

), sub_combo AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['subscription_info.ping_created_date_month', 'metrics.metrics_path', 'metrics.ping_edition', 'subscription_info.ping_deployment_type']) }}
                                                                                                                                          AS ping_subscriptions_reported_counts_monthly_id,
      subscription_info.ping_created_date_month                                                                                           AS ping_created_date_month,
      subscription_info.ping_delivery_type                                                                                                AS ping_delivery_type,
      subscription_info.ping_deployment_type                                                                                              AS ping_deployment_type,
      metrics.metrics_path                                                                                                                AS metrics_path,
      metrics.ping_edition                                                                                                                AS ping_edition,
      subscription_info.total_subscription_count                                                                                          AS total_subscription_count,
      subscription_info.total_licensed_users                                                                                              AS total_licensed_users
    FROM subscription_info
        INNER JOIN metrics
    ON subscription_info.key = metrics.key
      AND subscription_info.ping_created_date_month = metrics.ping_created_date_month

)

 {{ dbt_audit(
     cte_ref="sub_combo",
     created_by="@icooper-acp",
     updated_by="@jpeguero",
     created_date="2022-04-07",
     updated_date="2023-06-26"
 ) }}
