{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_namespace_metric_pk",
    on_schema_change='sync_all_columns'
) }}

WITH database_metrics AS (

  SELECT
    saas_usage_ping_gitlab_dotcom_namespace_id,
    dim_namespace_id::VARCHAR AS dim_namespace_id,
    ping_name,
    {{ dbt_utils.generate_surrogate_key(['ping_name']) }} AS ping_metric_id,
    ping_date,
    counter_value
  FROM {{ ref('prep_saas_usage_ping_namespace') }}
  {% if is_incremental() %}
  WHERE ping_date >= (SELECT MAX(ping_created_at) FROM {{this}})
  {% endif %}


),

internal_events_metrics AS (

  SELECT
    saas_usage_ping_gitlab_dotcom_namespace_id,
    dim_namespace_id::VARCHAR AS dim_namespace_id,
    ping_name,
    {{ dbt_utils.generate_surrogate_key(['ping_name']) }} AS ping_metric_id,
    ping_date,
    counter_value
  FROM {{ ref('prep_internal_events_ping_namespace') }}
  {% if is_incremental() %}
  WHERE ping_date >= (SELECT MAX(ping_created_at) FROM {{this}})
  {% endif %}

),

prep_date AS (

  SELECT
    date_id::VARCHAR AS date_id,
    date_day
  FROM {{ ref('prep_date') }}

),

unioned AS (
  SELECT
    *
  FROM database_metrics
  
  UNION All
  
  SELECT
    *
  FROM internal_events_metrics
)

SELECT
  --Primary Key
  saas_usage_ping_gitlab_dotcom_namespace_id AS ping_namespace_metric_pk,

  --Foreign Keys
  {{ get_keyed_nulls('dim_namespace_id') }}  AS dim_namespace_id,
  {{ get_keyed_nulls('ping_metric_id') }}    AS ping_metric_id,
  {{ get_keyed_nulls('date_id') }}           AS dim_ping_date_id,

  --Time attributes
  unioned.ping_date                          AS ping_created_at,

  --Dimensional value
  unioned.ping_name                          AS metrics_path,

  --Measurement
  unioned.counter_value                      AS metric_value

FROM unioned
LEFT JOIN prep_date
  ON TO_DATE(unioned.ping_date) = prep_date.date_day
