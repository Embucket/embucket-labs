{{ config(
  materialized='view',
  snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH date_details AS (

    SELECT *
    FROM {{ ref("date_details") }}
    WHERE last_day_of_month = date_actual

), namespace_statistics_snapshots AS (

    SELECT
      *,
      IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
    FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics_snapshots_base') }}

), namespace_statistics_snapshots_monthly AS (

    SELECT
      DATE_TRUNC('month', date_details.date_actual) AS snapshot_month,
      namespace_statistics_snapshots.namespace_id,
      namespace_statistics_snapshots.repository_size,
      namespace_statistics_snapshots.container_registry_size,
      namespace_statistics_snapshots.lfs_objects_size,
      namespace_statistics_snapshots.wiki_size,
      namespace_statistics_snapshots.build_artifacts_size,
      namespace_statistics_snapshots.storage_size,
      namespace_statistics_snapshots.packages_size
    FROM namespace_statistics_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN namespace_statistics_snapshots.valid_from AND namespace_statistics_snapshots.valid_to_
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, namespace_id ORDER BY valid_to_ DESC) = 1

)

SELECT *
FROM namespace_statistics_snapshots_monthly
