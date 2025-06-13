{{ config(
    tags=["mnpi_exception", "product"],
    materialized="table"
) }}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('dim_ping_metric'), where="is_health_score_metric = TRUE", column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_saas_usage_ping_namespace','prep_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('bdg_namespace_order','bdg_namespace_order_subscription_monthly')
]) }}

, health_score_metrics AS (
    SELECT metrics_path
    FROM {{ ref('dim_ping_metric') }}
    WHERE is_health_score_metric = TRUE
)

, free_namespaces AS (

    SELECT *
    FROM bdg_namespace_order
    WHERE dim_namespace_id IS NOT NULL
      AND (dim_order_id IS NULL
           OR order_is_trial = TRUE)

), joined AS (

    SELECT
      prep_saas_usage_ping_namespace.dim_namespace_id,
      prep_saas_usage_ping_namespace.ping_date,
      prep_saas_usage_ping_namespace.ping_name,
      prep_saas_usage_ping_namespace.counter_value,
      dim_date.first_day_of_month                           AS reporting_month,
      free_namespaces.dim_subscription_id,
      free_namespaces.dim_crm_account_id
    FROM prep_saas_usage_ping_namespace
    INNER JOIN dim_date
      ON prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    INNER JOIN free_namespaces
      ON prep_saas_usage_ping_namespace.dim_namespace_id = free_namespaces.dim_namespace_id
      AND dim_date.first_day_of_month = free_namespaces.snapshot_month
    INNER JOIN health_score_metrics
      ON prep_saas_usage_ping_namespace.ping_name = health_score_metrics.metrics_path
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_date.first_day_of_month,
        prep_saas_usage_ping_namespace.dim_namespace_id,
        prep_saas_usage_ping_namespace.ping_name
      ORDER BY prep_saas_usage_ping_namespace.ping_date DESC
    ) = 1

), pivoted AS (

    SELECT
      dim_namespace_id,
      dim_subscription_id,
      dim_crm_account_id,
      reporting_month,
      MAX(ping_date)                                        AS ping_date,
      {{ dbt_utils.pivot('ping_name', gainsight_wave_metrics, then_value='counter_value') }}
    FROM joined
    {{ dbt_utils.group_by(n=4)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@ischweickartDD",
    updated_by="@utkarsh060",
    created_date="2021-06-04",
    updated_date="2024-08-01"
) }}
