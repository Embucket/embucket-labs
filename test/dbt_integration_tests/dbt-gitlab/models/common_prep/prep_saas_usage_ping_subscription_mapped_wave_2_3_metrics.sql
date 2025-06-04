{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('dim_ping_metric'), where="is_health_score_metric = TRUE", column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('fct_ping_namespace_metric','fct_ping_namespace_metric'),
    ('dim_date','dim_date'),
    ('bdg_namespace_subscription','bdg_namespace_order_subscription_monthly'),
    ('instance_types', 'dim_host_instance_type'),
    ('map_subscription_namespace_month', 'map_latest_subscription_namespace_monthly')
]) }}

, health_score_metrics AS (
    SELECT metrics_path
    FROM {{ ref('dim_ping_metric') }}
    WHERE is_health_score_metric = TRUE
)

, instance_types_ordering AS (
    SELECT
      *,
      CASE
        WHEN instance_type = 'Production' THEN 1
        WHEN instance_type = 'Non-Production' THEN 2
        WHEN instance_type = 'Unknown' THEN 3
        ELSE 4
      END AS instance_type_ordering_field,
      /*
        Adding an ordering field to account for namespace that are associated with multiple production installations
      */
      CASE
        WHEN included_in_health_measures_str = 'Included in Health Score' THEN 1
        WHEN included_in_health_measures_str = 'Opt-Out' THEN 2
        WHEN included_in_health_measures_str = NULL THEN 3
      END AS health_score_ordering_field
    FROM instance_types
)

, namespace_subscription_monthly_distinct AS (

    SELECT DISTINCT
      dim_namespace_id,
      dim_subscription_id,
      dim_subscription_id_original,
      snapshot_month,
      subscription_version
    FROM bdg_namespace_subscription
    WHERE namespace_order_subscription_match_status = 'Paid All Matching'
)

, joined AS (

    SELECT
      fct_ping_namespace_metric.dim_namespace_id,
      fct_ping_namespace_metric.ping_created_at AS ping_date,
      fct_ping_namespace_metric.metrics_path AS ping_name,
      fct_ping_namespace_metric.metric_value AS counter_value,
      dim_date.first_day_of_month                           AS reporting_month,
      COALESCE(
        map_subscription_namespace_month.dim_subscription_id,
        namespace_subscription_monthly_distinct.dim_subscription_id
      ) AS dim_subscription_id,
      instance_types_ordering.instance_type,
      instance_types_ordering.included_in_health_measures_str
    FROM fct_ping_namespace_metric
    LEFT JOIN instance_types_ordering
      ON fct_ping_namespace_metric.dim_namespace_id = instance_types_ordering.namespace_id::VARCHAR
    INNER JOIN dim_date
      ON fct_ping_namespace_metric.ping_created_at = dim_date.date_day
    LEFT JOIN namespace_subscription_monthly_distinct
      ON fct_ping_namespace_metric.dim_namespace_id = namespace_subscription_monthly_distinct.dim_namespace_id::VARCHAR
      AND dim_date.first_day_of_month = namespace_subscription_monthly_distinct.snapshot_month
    INNER JOIN health_score_metrics
      ON fct_ping_namespace_metric.metrics_path = health_score_metrics.metrics_path
    LEFT JOIN map_subscription_namespace_month
      ON fct_ping_namespace_metric.dim_namespace_id = map_subscription_namespace_month.dim_namespace_id::VARCHAR
      AND dim_date.first_day_of_month = map_subscription_namespace_month.date_month
    WHERE COALESCE(
        map_subscription_namespace_month.dim_subscription_id,
        namespace_subscription_monthly_distinct.dim_subscription_id
      ) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_date.first_day_of_month,
        COALESCE(
          map_subscription_namespace_month.dim_subscription_id,
          namespace_subscription_monthly_distinct.dim_subscription_id
        ),
        fct_ping_namespace_metric.dim_namespace_id,
        fct_ping_namespace_metric.metrics_path
        ORDER BY
          fct_ping_namespace_metric.ping_created_at DESC,
          instance_types_ordering.instance_type_ordering_field ASC, --prioritizing Production instances
          instance_types_ordering.health_score_ordering_field ASC
    ) = 1

), pivoted AS (

    SELECT
      dim_namespace_id,
      dim_subscription_id,
      reporting_month,
      instance_type,
      included_in_health_measures_str,
      MAX(ping_date)                                        AS ping_date,
      {{ dbt_utils.pivot('ping_name', gainsight_wave_metrics, then_value='counter_value') }}
    FROM joined
    {{ dbt_utils.group_by(n=5)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@mpeychet_",
    updated_by="@utkarsh060",
    created_date="2021-03-22",
    updated_date="2024-08-01"
) }}
