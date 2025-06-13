{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}},


fct_ping_instance_metric AS (

  SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
    TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
  FROM {{ ref('fct_ping_instance_metric') }}

),

filtered_fct_ping_instance_metric AS (
  SELECT
    fct_ping_instance_metric.*,
    dim_ping_metric.time_frame,
    dim_ping_instance.is_last_ping_of_month AS is_current_ping,
    dim_ping_instance.ping_created_date_month
  FROM fct_ping_instance_metric
  INNER JOIN dim_ping_metric
    ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
  INNER JOIN dim_ping_instance
    ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
  WHERE dim_ping_metric.time_frame IN ('28d', 'all')
    AND dim_ping_instance.is_last_ping_of_month = TRUE
    AND fct_ping_instance_metric.has_timed_out = FALSE
    AND fct_ping_instance_metric.metric_value IS NOT NULL
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['dim_installation_id', 'metrics_path', 'ping_created_date_month']) }} AS ping_instance_metric_monthly_pk,
    ping_instance_metric_id,
    dim_ping_instance_id,
    dim_product_tier_id,
    dim_subscription_id,
    dim_location_country_id,
    dim_ping_date_id,
    dim_instance_id,
    dim_host_id,
    dim_installation_id,
    dim_license_id,
    dim_subscription_license_id,
    dim_app_release_major_minor_sk,
    dim_latest_available_app_release_major_minor_sk,
    metrics_path,
    metric_value,
    CASE time_frame
      WHEN 'all'
        THEN GREATEST(metric_value - LAG(metric_value, 1, 0)
            OVER (PARTITION BY dim_installation_id, metrics_path ORDER BY ping_created_at ASC), 0)
      WHEN '28d' THEN metric_value
    END AS monthly_metric_value,
    time_frame,
    has_timed_out,
    ping_created_at,
    umau_value,
    data_source,
    uploaded_at,
    is_current_ping
  FROM filtered_fct_ping_instance_metric
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@pempey",
    created_date="2022-05-09",
    updated_date="2024-10-01"
) }}
