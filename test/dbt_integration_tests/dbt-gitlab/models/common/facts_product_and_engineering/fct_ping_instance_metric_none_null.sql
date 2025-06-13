{{ config(
  tags=["product", "mnpi_exception"],
  snowflake_warehouse=generate_warehouse_name('XL'),
  materialized='incremental',
  unique_key=['dim_ping_instance_id', 'metrics_path']
) }}

{{ simple_cte([
  ('dim_ping_metric', 'dim_ping_metric')
]) }}, 

fct_ping_instance_metric AS (
  SELECT
    {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM {{ ref('fct_ping_instance_metric') }}
  {% if is_incremental() %}
    WHERE ping_created_at >= (
      SELECT MAX(ping_created_at)
      FROM {{ this }}
    )
  {% endif %}
),

final AS (
  SELECT
    fct_ping_instance_metric.*,
    dim_ping_metric.time_frame
  FROM fct_ping_instance_metric
  LEFT JOIN dim_ping_metric
    ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
  WHERE dim_ping_metric.time_frame = 'none'
    OR dim_ping_metric.time_frame IS NULL
)

SELECT *
FROM final