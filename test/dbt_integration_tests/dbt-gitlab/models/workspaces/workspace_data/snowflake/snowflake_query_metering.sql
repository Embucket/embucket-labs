{{
  config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH source AS (
  SELECT *
  FROM {{ ref('snowflake_query_spend_attribution') }}
  {% if is_incremental() %}

    WHERE query_end_at > (SELECT MAX(query_end_at) FROM {{ this }})

  {% endif %}
),

query_aggregation AS (

  SELECT
    query_id,
    query_start_at,
    query_end_at,
    SUM(attributed_query_credits) AS total_attributed_credits
  FROM source
  GROUP BY 1, 2, 3
)

SELECT *
FROM query_aggregation
