{% set filter_date = (run_started_at - modules.datetime.timedelta(days=8)).strftime('%Y-%m-%d') %}

{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id",
    on_schema_change="sync_all_columns",
    post_hook=["DELETE FROM {{ this }} WHERE is_current_ping = FALSE"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
      TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
    FROM {{ ref('fct_ping_instance_metric') }}
    {% if is_incremental() %}
    WHERE uploaded_at > '{{ filter_date }}'
    {% endif %}

),

time_frame_7_day_metrics AS (

    SELECT
      fct_ping_instance_metric.*,
      dim_ping_metric.time_frame,
      dim_ping_instance.is_last_ping_of_week AS is_current_ping
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    WHERE dim_ping_metric.time_frame = ('7d')
      {% if is_incremental() %}
      AND dim_ping_instance.next_ping_uploaded_at > '{{ filter_date }}'
      {% else %}
      -- Only filtered on a full refresh as the post_hook DELETE step applies the filter during an incremental build
      AND dim_ping_instance.is_last_ping_of_week = TRUE
      {% endif %}
      AND fct_ping_instance_metric.has_timed_out = FALSE
      AND fct_ping_instance_metric.metric_value IS NOT NULL

)

{{ dbt_audit(
    cte_ref="time_frame_7_day_metrics",
    created_by="@iweeks",
    updated_by="@pempey",
    created_date="2022-08-08",
    updated_date="2024-05-21"
) }}
