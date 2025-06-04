{{
  config(
    materialized='incremental',
    unique_key='attribution_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH
query_history AS (
  SELECT *
  FROM {{ ref('snowflake_query_history_source') }}
  {% if is_incremental() %}

    WHERE query_end_at > (SELECT MAX(query_end_at) FROM {{ this }})

  {% endif %}
),

warehouse_spend AS (
  SELECT *
  FROM {{ ref('snowflake_warehouse_metering_history_source') }}
),

attribution AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'query_history.query_id',
      'warehouse_spend.warehouse_metering_start_at']) }}                                     AS attribution_id,
    query_history.query_id,
    query_history.warehouse_id,
    query_history.query_start_at,
    DATEADD('millisecond', -(query_history.execution_time), query_history.query_end_at)      AS query_execution_start_at,
    query_history.query_end_at,
    query_history.execution_time,
    query_history.total_elapsed_time,
    warehouse_spend.warehouse_metering_start_at                                              AS spend_start_at,
    warehouse_spend.warehouse_metering_end_at                                                AS spend_end_at,
    warehouse_spend.credits_used_total,

    -- Check the type of overlap
    IFF(query_execution_start_at >= spend_start_at
      AND query_end_at > spend_end_at, TRUE, FALSE)                                          AS start_during_end_after,
    IFF(query_execution_start_at < spend_start_at
      AND query_end_at > spend_end_at, TRUE, FALSE)                                          AS start_before_end_after,
    IFF(query_execution_start_at < spend_start_at
      AND query_end_at <= spend_end_at, TRUE, FALSE)                                         AS start_before_end_during,
    IFF(query_execution_start_at > spend_start_at
      AND query_end_at < spend_end_at, TRUE, FALSE)                                          AS start_during_end_during,

    -- Calculating query time in spend window
    CASE
      WHEN start_during_end_after THEN DATEDIFF('millisecond', query_execution_start_at, spend_end_at)
      WHEN start_before_end_after THEN DATEDIFF('millisecond', spend_start_at, spend_end_at)
      WHEN start_before_end_during THEN DATEDIFF('millisecond', spend_start_at, query_end_at)
      WHEN start_during_end_during THEN DATEDIFF('millisecond', query_execution_start_at, query_end_at)
    END                                                                                      AS query_spend_duration,

    -- Attribution calculations
    SUM(query_spend_duration) OVER (PARTITION BY query_history.warehouse_id, spend_start_at) AS total_query_duration,
    query_spend_duration / NULLIF(total_query_duration, 0)                                   AS query_spend_fraction,
    query_spend_fraction * warehouse_spend.credits_used_total                                AS attributed_query_credits
  FROM query_history
  INNER JOIN warehouse_spend
    ON query_history.warehouse_id = warehouse_spend.warehouse_id
      AND NOT (spend_end_at <= query_execution_start_at OR spend_start_at >= query_end_at) -- Check for overlap

)

SELECT *
FROM attribution
