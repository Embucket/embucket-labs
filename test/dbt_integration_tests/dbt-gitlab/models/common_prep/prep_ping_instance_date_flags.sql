{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

WITH
source AS (
  SELECT *
  FROM {{ ref('prep_ping_instance') }}
),

current_flags AS (

  SELECT
    dim_ping_instance_id,
    DATE_TRUNC('week',source.ping_created_at)                                 AS ping_created_week,
    DATE_TRUNC('month',source.ping_created_at)                                AS ping_created_month,
    IFF(ROW_NUMBER() OVER (
      PARTITION BY source.uuid, source.host_id, ping_created_month
      ORDER BY source.ping_created_at DESC, source.id DESC) = 1, TRUE, FALSE) AS last_ping_of_month_flag,
    IFF(ROW_NUMBER() OVER (
      PARTITION BY source.uuid, source.host_id, ping_created_week
      ORDER BY source.ping_created_at DESC, source.id DESC) = 1, TRUE, FALSE) AS last_ping_of_week_flag,
    LAG(uploaded_at,1,uploaded_at) OVER (
      PARTITION BY source.uuid, source.host_id
      ORDER BY source.ping_created_at DESC, source.id DESC)                   AS next_ping_uploaded_at,
    CONDITIONAL_TRUE_EVENT(next_ping_uploaded_at != source.uploaded_at) OVER (
      PARTITION BY source.uuid, source.host_id
      ORDER BY ping_created_at DESC, source.id DESC)                          AS uploaded_group
  FROM source

)

SELECT *
FROM current_flags
