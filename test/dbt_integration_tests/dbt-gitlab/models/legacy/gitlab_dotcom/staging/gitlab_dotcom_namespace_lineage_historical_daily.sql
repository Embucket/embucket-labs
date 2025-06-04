{{ config({
    "materialized": "incremental",
    "unique_key": "snapshot_day_namespace_id",
    "on_schema_change":"sync_all_columns"
    })
}}



{{ simple_cte([
    ('map_namespace_internal', 'map_namespace_internal'),
    ('namespace_subscription_snapshots', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('namespace_lineage','gitlab_dotcom_namespace_lineage_scd'),
    ('prep_gitlab_dotcom_plan','prep_gitlab_dotcom_plan')
])}}

, dates AS (
  SELECT *
  FROM {{ ref('dim_date') }} --prod.common.dim_date
  WHERE date_actual <= CURRENT_DATE()
  {% if is_incremental() -%}
  AND date_actual >= (SELECT MAX(snapshot_day) FROM {{ this }})
  {%- endif %}

), namespace_lineage_daily AS (

  SELECT
    dates.date_actual AS snapshot_day,
    namespace_lineage.namespace_id,
    namespace_lineage.parent_id,
    namespace_lineage.upstream_lineage,
    namespace_lineage.ultimate_parent_id
  FROM namespace_lineage
  INNER JOIN dates
    ON dates.date_actual BETWEEN date_trunc('day',namespace_lineage.lineage_valid_from) AND date_trunc('day',namespace_lineage.lineage_valid_to)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      dates.date_actual,
      namespace_id 
    ORDER BY namespace_lineage.lineage_valid_to DESC
    ) = 1

), with_plans AS (

    SELECT
      namespace_lineage_daily.*,
      IFNULL(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL, FALSE)  AS namespace_is_internal,
      --Please see dbt docs for a description of this column transformation.
      CASE
        WHEN namespace_subscription_snapshots.is_trial = TRUE AND LOWER(prep_gitlab_dotcom_plan.plan_name_modified) = 'ultimate' THEN 102
        WHEN namespace_subscription_snapshots.plan_id IS NULL THEN 34
        ELSE prep_gitlab_dotcom_plan.plan_id_modified
      END AS ultimate_parent_plan_id,
      namespace_subscription_snapshots.seats,
      namespace_subscription_snapshots.seats_in_use,
      namespace_subscription_snapshots.max_seats_used,
      CASE 
        WHEN namespace_is_internal = TRUE
          THEN 'internal'
        WHEN namespace_subscription_snapshots.is_trial = TRUE
          THEN 'free'
        WHEN prep_gitlab_dotcom_plan.plan_is_paid = TRUE
          THEN 'paid'
        ELSE 'free'
      END                                                   AS finance_pl_category,
    FROM namespace_lineage_daily
    LEFT JOIN map_namespace_internal
      ON namespace_lineage_daily.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
    LEFT JOIN namespace_subscription_snapshots
      ON namespace_lineage_daily.ultimate_parent_id = namespace_subscription_snapshots.namespace_id
      AND namespace_lineage_daily.snapshot_day BETWEEN namespace_subscription_snapshots.valid_from::DATE AND IFNULL(namespace_subscription_snapshots.valid_to::DATE, CURRENT_DATE)
    LEFT JOIN prep_gitlab_dotcom_plan
      ON namespace_subscription_snapshots.plan_id = prep_gitlab_dotcom_plan.dim_plan_id
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY
        namespace_lineage_daily.namespace_id,
        snapshot_day
      ORDER BY valid_from DESC
      ) = 1

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['snapshot_day', 'namespace_id'] ) }}                    AS snapshot_day_namespace_id,
  *
FROM with_plans
