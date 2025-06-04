WITH date_details AS (

  SELECT *
  FROM {{ ref('date_details') }}
  WHERE last_day_of_month = date_actual

), gitlab_dotcom_namespace_historical_monthly AS (

    SELECT
      date_details.first_day_of_month AS snapshot_month,
      gitlab_dotcom_namespace_historical_daily.namespace_id,
      gitlab_dotcom_namespace_historical_daily.parent_id,
      gitlab_dotcom_namespace_historical_daily.owner_id,
      gitlab_dotcom_namespace_historical_daily.namespace_type,
      gitlab_dotcom_namespace_historical_daily.visibility_level,
      gitlab_dotcom_namespace_historical_daily.shared_runners_minutes_limit,
      gitlab_dotcom_namespace_historical_daily.extra_shared_runners_minutes_limit,
      gitlab_dotcom_namespace_historical_daily.repository_size_limit
    FROM {{ ref('gitlab_dotcom_namespace_historical_daily') }}
    INNER JOIN date_details
      ON date_details.date_actual = gitlab_dotcom_namespace_historical_daily.snapshot_day

), project_snapshot_monthly_all AS (

    --project_snapshot_monthly 
    SELECT
      snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM {{ ref('gitlab_dotcom_project_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL
      
    --project_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                         AS snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM {{ ref('gitlab_dotcom_projects_source') }}
    
), namespace_lineage_monthly_all AS (

    --namespace_lineage_monthly
    SELECT
      snapshot_month,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL
    
    --namespace_lineage_current
    SELECT
      DATE_TRUNC('month', snapshot_day)                         AS snapshot_month,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_daily') }}
    WHERE snapshot_day = DATEADD('DAY', -1, CURRENT_DATE)
      AND IFF(DAY(CURRENT_DATE) = 1, FALSE, TRUE) -- If it is the first day of the month, do not return lineage as it will conflict with the statement above


), namespace_snapshots_monthly_all AS (

    --namespace_snapshots_monthly
    SELECT
      snapshot_month,
      namespace_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM gitlab_dotcom_namespace_historical_monthly
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL
    
    --namespace_current
    SELECT 
      DATE_TRUNC('month', CURRENT_DATE)                         AS snapshot_month,
      namespace_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM {{ ref('gitlab_dotcom_namespaces_source') }} 

), namespace_statistics_monthly_all AS (

    --namespace_statistics_monthly
    SELECT 
      snapshot_month,
      namespace_id,
      shared_runners_seconds,
      shared_runners_seconds_last_reset
    FROM {{ ref('gitlab_dotcom_namespace_statistics_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL

    --namespace_statistics_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                         AS snapshot_month,
      namespace_id,
      shared_runners_seconds,
      shared_runners_seconds_last_reset
    FROM {{ ref('gitlab_dotcom_namespace_statistics_source') }}
      
), child_projects_enabled_shared_runners_any AS (

    SELECT
      project_snapshot_monthly_all.snapshot_month,
      namespace_lineage_monthly_all.ultimate_parent_id,
      MAX(project_snapshot_monthly_all.shared_runners_enabled)  AS shared_runners_enabled
    FROM project_snapshot_monthly_all
    INNER JOIN namespace_lineage_monthly_all
      ON project_snapshot_monthly_all.namespace_id = namespace_lineage_monthly_all.namespace_id
      AND project_snapshot_monthly_all.snapshot_month = namespace_lineage_monthly_all.snapshot_month
    GROUP BY 1, 2

), namespace_statistics_monthly_top_level AS (

    SELECT
      namespace_snapshots_monthly_all.snapshot_month            AS namespace_snapshots_snapshot_month,
      namespace_snapshots_monthly_all.namespace_id              AS namespace_snapshots_namespace_id,
      namespace_snapshots_monthly_all.parent_id,
      namespace_snapshots_monthly_all.owner_id,
      namespace_snapshots_monthly_all.namespace_type,
      namespace_snapshots_monthly_all.visibility_level,
      namespace_snapshots_monthly_all.shared_runners_minutes_limit,
      namespace_snapshots_monthly_all.extra_shared_runners_minutes_limit,
      namespace_statistics_monthly_all.snapshot_month            AS namespace_statistics_snapshot_month,
      namespace_statistics_monthly_all.namespace_id              AS namespace_statistics_namespace_id,
      namespace_statistics_monthly_all.shared_runners_seconds,
      namespace_statistics_monthly_all.shared_runners_seconds_last_reset
    FROM namespace_snapshots_monthly_all
    LEFT JOIN namespace_statistics_monthly_all
      ON namespace_snapshots_monthly_all.namespace_id = namespace_statistics_monthly_all.namespace_id
      AND namespace_snapshots_monthly_all.snapshot_month = namespace_statistics_monthly_all.snapshot_month
      AND namespace_snapshots_monthly_all.parent_id IS NULL  -- Only top level namespaces
      
), ci_minutes_logic AS (
    
    SELECT
      namespace_statistics_monthly_top_level.namespace_snapshots_snapshot_month
                                                                AS snapshot_month,
      namespace_statistics_monthly_top_level.namespace_snapshots_namespace_id
                                                                AS namespace_id,
      IFNULL(child_projects_enabled_shared_runners_any.ultimate_parent_id,
             namespace_id)                                      AS ultimate_parent_namespace_id,
      namespace_statistics_monthly_top_level.namespace_type,
      namespace_statistics_monthly_top_level.visibility_level,
      IFNULL(child_projects_enabled_shared_runners_any.shared_runners_enabled,
             False)                                             AS shared_runners_enabled,
      IFF(snapshot_month >= '2020-10-01',
          400, 2000)                                            AS gitlab_current_settings_shared_runners_minutes,
      IFNULL(namespace_statistics_monthly_top_level.shared_runners_minutes_limit,
             gitlab_current_settings_shared_runners_minutes)    AS monthly_minutes, 
      IFNULL(namespace_statistics_monthly_top_level.extra_shared_runners_minutes_limit,
             0)                                                 AS purchased_minutes,
      IFNULL(namespace_statistics_monthly_top_level.shared_runners_seconds / 60,
             0)                                                 AS total_minutes_used,
      IFF(purchased_minutes = 0
            OR total_minutes_used < monthly_minutes,
          0, total_minutes_used - monthly_minutes)              AS purchased_minutes_used,
      total_minutes_used - purchased_minutes_used               AS monthly_minutes_used,    
      IFF(shared_runners_enabled
            AND monthly_minutes != 0,
          True, False)                                          AS shared_runners_minutes_limit_enabled,
      CASE 
        WHEN shared_runners_minutes_limit_enabled
          THEN monthly_minutes::VARCHAR
        WHEN monthly_minutes = 0
          THEN 'Unlimited minutes'
        ELSE 'Not supported minutes'
      END                                                       AS limit,
      IFF(monthly_minutes != 0,
          monthly_minutes, NULL)                                AS limit_based_plan,
      CASE
        WHEN NOT shared_runners_minutes_limit_enabled
          THEN 'Disabled'
        WHEN monthly_minutes_used < monthly_minutes
          THEN 'Under Quota'
        ELSE 'Over Quota'
      END                                                       AS status,
      IFF(monthly_minutes_used < monthly_minutes
            OR monthly_minutes = 0,
          'Under Quota', 'Over Quota')                          AS status_based_plan,
      IFF(purchased_minutes_used <= purchased_minutes
            OR NOT shared_runners_minutes_limit_enabled,
          'Under Quota', 'Over Quota')                          AS status_purchased
    FROM namespace_statistics_monthly_top_level
    LEFT JOIN child_projects_enabled_shared_runners_any
      ON namespace_statistics_monthly_top_level.namespace_snapshots_namespace_id = child_projects_enabled_shared_runners_any.ultimate_parent_id
      AND namespace_statistics_monthly_top_level.namespace_snapshots_snapshot_month = child_projects_enabled_shared_runners_any.snapshot_month

), final AS (

    SELECT
      snapshot_month,
      namespace_id                                              AS dim_namespace_id,
      ultimate_parent_namespace_id,
      namespace_type,
      visibility_level, 
      limit,
      total_minutes_used                                        AS shared_runners_minutes_used_overall,  
      status,
      limit_based_plan,
      monthly_minutes_used                                      AS used,
      status_based_plan,
      purchased_minutes                                         AS limit_purchased,
      purchased_minutes_used                                    AS used_purchased, 
      status_purchased
    FROM ci_minutes_logic

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2020-12-31",
    updated_date="2021-06-17"
) }}