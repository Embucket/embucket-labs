{{ config({
    "materialized": "incremental",
    "unique_key": "snapshot_day_namespace_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH date_details AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE date_actual <= CURRENT_DATE
    {% if is_incremental() %}
      AND date_actual >= (SELECT MAX(snapshot_day) FROM {{ this }})
    {% endif %}

), namespace_snapshots AS (

    SELECT
      *,
      IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
    FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }}
    {% if is_incremental() %}
    WHERE (SELECT MAX(snapshot_day) FROM {{ this }}) BETWEEN valid_from AND valid_to_
    {% endif %}

), namespace_snapshots_daily AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['date_actual', 'namespace_id']) }}           AS snapshot_day_namespace_id,
      date_details.date_actual                                                 AS snapshot_day,
      namespace_snapshots.namespace_id,
      namespace_snapshots.parent_id,
      namespace_snapshots.owner_id,
      namespace_snapshots.namespace_type,
      namespace_snapshots.visibility_level,
      namespace_snapshots.shared_runners_minutes_limit,
      namespace_snapshots.extra_shared_runners_minutes_limit,
      namespace_snapshots.repository_size_limit,
      namespace_snapshots.namespace_created_at
    FROM namespace_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN namespace_snapshots.valid_from::DATE AND namespace_snapshots.valid_to_::DATE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_day, namespace_id ORDER BY valid_to_ DESC) = 1

)

SELECT *
FROM namespace_snapshots_daily
