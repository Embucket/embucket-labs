{{ config(
  materialized='view',
  snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH date_details AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE last_day_of_month = date_actual

), namespace_lineage_snapshots_daily AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_daily') }}

), namespace_lineage_snapshots_monthly AS (

    SELECT
      date_details.first_day_of_month AS snapshot_month,
      namespace_lineage_snapshots_daily.namespace_id,
      namespace_lineage_snapshots_daily.parent_id,
      namespace_lineage_snapshots_daily.upstream_lineage,
      namespace_lineage_snapshots_daily.ultimate_parent_id,
      namespace_lineage_snapshots_daily.namespace_is_internal,
      namespace_lineage_snapshots_daily.ultimate_parent_plan_id
    FROM namespace_lineage_snapshots_daily
    INNER JOIN date_details
      ON date_details.date_actual = namespace_lineage_snapshots_daily.snapshot_day

)

SELECT *
FROM namespace_lineage_snapshots_monthly
