{{
  config(
    materialized='table'
  )
}}

{{ simple_cte([
  ('initial_export', 'rally_initial_export_optouts_source'),
  ('webhook', 'rally_webhook_stitch_optouts_source')
]) }},

combined AS (
  SELECT *
  FROM initial_export
  UNION ALL
  SELECT *
  FROM webhook
),

dedupped AS (
  SELECT *
  FROM combined
  QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated_at DESC) = 1
)


SELECT *
FROM dedupped
