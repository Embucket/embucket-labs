{% set bytes_to_gib_conversion = 1073741824 %} -- To convert storage (usage) sizes from bytes in source to GiB for reporting (1 GiB = 2^30 bytes = 1,073,741,824 bytes)
{% set bytes_to_mib_conversion = 1048576 %} -- To convert storage (usage) sizes from bytes in source to MiB for reporting (1 MiB = 2^20 bytes = 1,048,576 bytes)
{% set mib_to_gib_conversion = 1024 %} -- To convert storage limit sizes from GiB in "source" to MiB for reporting (1 GiB = 1024 MiB)

WITH namespace_current AS (

    SELECT dim_namespace_id
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE
  
), project_statistics_snapshot_monthly_all AS (

    --project_statistics_snapshot_monthly 
    SELECT
      snapshot_month,
      project_id,
      namespace_id,
      (repository_size + lfs_objects_size) / {{bytes_to_gib_conversion}}        AS project_storage_size
    FROM {{ ref('gitlab_dotcom_project_statistic_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL
      
    --project_statistics_current
    SELECT 
      DATE_TRUNC('month', CURRENT_DATE)                                         AS snapshot_month,
      project_id,
      namespace_id,
      (repository_size + lfs_objects_size) / {{bytes_to_gib_conversion}}        AS project_storage_size
    FROM {{ ref('gitlab_dotcom_project_statistics_source') }}

), namespace_lineage_monthly_all AS (

    --namespace_lineage_monthly
    SELECT
      snapshot_month,
      namespace_id,
      ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)

    UNION ALL
    
    --namespace_lineage_current
    SELECT
      DATE_TRUNC('month', namespace_lineage.snapshot_day)                         AS snapshot_month,
      namespace_lineage.namespace_id                                              AS namespace_id,
      namespace_lineage.ultimate_parent_id                                        AS ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_daily') }} AS namespace_lineage
    INNER JOIN namespace_current -- To filter for non deleted namespaces
      ON namespace_lineage.namespace_id = namespace_current.dim_namespace_id
    WHERE namespace_lineage.snapshot_day = DATEADD('DAY', -1, CURRENT_DATE)
      AND IFF(DAY(CURRENT_DATE) = 1, FALSE, TRUE) -- If it is the first day of the month, do not return lineage as it will conflict with the statement above

), namespace_storage_statistic_monthly_all AS (

    --namespace_storage_statistic_monthly
    SELECT 
      snapshot_month,
      namespace_id,
      storage_size,
      repository_size,
      container_registry_size,
      lfs_objects_size,
      build_artifacts_size,
      packages_size,
      wiki_size,
      repository_size + lfs_objects_size                                        AS billable_storage_size
    FROM {{ ref('gitlab_dotcom_namespace_storage_statistics_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'
      AND snapshot_month < DATE_TRUNC('month', CURRENT_DATE)
    
    UNION ALL

    --namespace_storage_statistic_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                                         AS snapshot_month,
      namespace_id,
      storage_size,
      repository_size,
      container_registry_size,
      lfs_objects_size,
      build_artifacts_size,
      packages_size,
      wiki_size,
      repository_size + lfs_objects_size                                        AS billable_storage_size
    FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics_source') }}

), month_spine AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual = first_day_of_month

), purchased_storage AS (

    SELECT
      gitlab_namespace_id::INT                                                  AS namespace_id,
      month_spine.first_day_of_month                                            AS snapshot_month,
      SUM(order_quantity * 10)                                                  AS purchased_storage_gib
    FROM {{ ref('customers_db_orders_source') }}
    INNER JOIN month_spine
      ON month_spine.first_day_of_month BETWEEN DATE_TRUNC('month', order_start_date)
                                          AND DATEADD(MONTH, -1, DATE_TRUNC('month', order_end_date))
    WHERE product_rate_plan_id = '2c92a00f7279a6f5017279d299d01cf9' --only storage rate plan, 10GiB of storage
    GROUP BY 1, 2

), top_level_namespace_storage_summary AS (

    SELECT
      namespace_lineage_monthly_all.ultimate_parent_id,                         -- Only top level namespaces
      namespace_lineage_monthly_all.snapshot_month,
      SUM(COALESCE(purchased_storage.purchased_storage_gib, 0))                 AS purchased_storage_limit,
      SUM(namespace_storage_statistic_monthly_all.billable_storage_size)        AS billable_storage_size,
      SUM(namespace_storage_statistic_monthly_all.repository_size)              AS repository_size,
      SUM(namespace_storage_statistic_monthly_all.container_registry_size)      AS container_registry_size,
      SUM(namespace_storage_statistic_monthly_all.lfs_objects_size)             AS lfs_objects_size,
      SUM(namespace_storage_statistic_monthly_all.build_artifacts_size)         AS build_artifacts_size,
      SUM(namespace_storage_statistic_monthly_all.packages_size)                AS packages_size,
      SUM(namespace_storage_statistic_monthly_all.wiki_size)                    AS wiki_size,
      SUM(namespace_storage_statistic_monthly_all.storage_size)                 AS storage_size
    FROM namespace_lineage_monthly_all
    LEFT JOIN namespace_storage_statistic_monthly_all
      ON namespace_lineage_monthly_all.namespace_id = namespace_storage_statistic_monthly_all.namespace_id
      AND namespace_lineage_monthly_all.snapshot_month = namespace_storage_statistic_monthly_all.snapshot_month
    LEFT JOIN purchased_storage
      ON namespace_lineage_monthly_all.namespace_id = purchased_storage.namespace_id
      AND namespace_lineage_monthly_all.snapshot_month = purchased_storage.snapshot_month
    GROUP BY 1, 2

), repository_level_statistics AS (

    SELECT DISTINCT
      namespace_lineage_monthly_all.snapshot_month,
      namespace_lineage_monthly_all.ultimate_parent_id,
      project_statistics_snapshot_monthly_all.project_id,
      COALESCE(project_statistics_snapshot_monthly_all.project_storage_size, 0) AS repository_storage_size,
      IFF(namespace_lineage_monthly_all.ultimate_parent_id = 6543, 0, 10)       AS repository_size_limit,
      top_level_namespace_storage_summary.purchased_storage_limit,
      IFF(repository_storage_size < repository_size_limit
            OR repository_size_limit = 0,
          FALSE, TRUE)                                                          AS is_free_storage_used_up,
      IFF(NOT is_free_storage_used_up
            OR purchased_storage_limit = 0,
          repository_storage_size, repository_size_limit)                       AS free_storage_size,
      repository_storage_size - free_storage_size                               AS purchased_storage_size,
      SUM(purchased_storage_size)
        OVER(
             PARTITION BY
               namespace_lineage_monthly_all.ultimate_parent_id,
               namespace_lineage_monthly_all.snapshot_month
            )                                                                   AS total_purchased_storage_size,
      IFF(is_free_storage_used_up
            AND (purchased_storage_limit = 0
                  OR total_purchased_storage_size >= purchased_storage_limit),
          TRUE, FALSE)                                                          AS is_repository_capped
    FROM namespace_lineage_monthly_all
    LEFT JOIN top_level_namespace_storage_summary
      ON namespace_lineage_monthly_all.ultimate_parent_id = top_level_namespace_storage_summary.ultimate_parent_id
      AND namespace_lineage_monthly_all.snapshot_month = top_level_namespace_storage_summary.snapshot_month
    LEFT JOIN project_statistics_snapshot_monthly_all
      ON namespace_lineage_monthly_all.namespace_id = project_statistics_snapshot_monthly_all.namespace_id
      AND namespace_lineage_monthly_all.snapshot_month = project_statistics_snapshot_monthly_all.snapshot_month

), namespace_repository_storage_usage_summary AS (

    SELECT
      ultimate_parent_id,                           -- Only top level namespaces
      snapshot_month,
      MAX(repository_storage_size)                  AS largest_repository_size,
      SUM(purchased_storage_size)                   AS purchased_storage,
      SUM(repository_size_limit)                    AS free_limit,
      SUM(free_storage_size)                        AS free_storage,
      SUM(IFF(is_free_storage_used_up, 1, 0))       AS repositories_above_free_limit_count,
      SUM(IFF(is_repository_capped, 1, 0))          AS capped_repositories_count
    FROM repository_level_statistics
    GROUP BY 1, 2
    
), joined AS (
    
    SELECT
      repository.snapshot_month,
      repository.ultimate_parent_id                                             AS dim_namespace_id,
      repository.ultimate_parent_id                                             AS ultimate_parent_namespace_id,
      repository.largest_repository_size                                        AS largest_repository_size_gib,
      repository.free_limit                                                     AS total_free_storage_limit_gib,
      namespace.purchased_storage_limit                                         AS total_purchased_storage_limit_gib,
      IFF(repository.repositories_above_free_limit_count = 0, FALSE, TRUE)      AS has_repositories_above_free_limit,
      repository.repositories_above_free_limit_count,
      IFF(repository.capped_repositories_count = 0, FALSE, TRUE)                AS has_capped_repositories,
      repository.capped_repositories_count,
      repository.free_storage * {{bytes_to_gib_conversion}}                     AS total_free_storage_bytes,
      repository.purchased_storage * {{bytes_to_gib_conversion}}                AS total_purchased_storage_bytes,
      namespace.billable_storage_size                                           AS billable_storage_bytes,
      namespace.repository_size                                                 AS repository_bytes,
      namespace.container_registry_size                                         AS container_registry_bytes,
      namespace.lfs_objects_size                                                AS lfs_objects_bytes,
      namespace.build_artifacts_size                                            AS build_artifacts_bytes,
      namespace.packages_size                                                   AS packages_bytes,
      namespace.wiki_size                                                       AS wiki_bytes,
      namespace.storage_size                                                    AS storage_bytes,
      repository.free_storage * {{mib_to_gib_conversion}}                       AS total_free_storage_mib,
      repository.purchased_storage * {{mib_to_gib_conversion}}                  AS total_purchased_storage_mib,
      namespace.billable_storage_size / {{bytes_to_mib_conversion}}             AS billable_storage_mib,
      namespace.repository_size / {{bytes_to_mib_conversion}}                   AS repository_mib,
      namespace.lfs_objects_size / {{bytes_to_mib_conversion}}                  AS lfs_objects_mib,
      namespace.build_artifacts_size / {{bytes_to_mib_conversion}}              AS build_artifacts_mib,
      namespace.packages_size / {{bytes_to_mib_conversion}}                     AS packages_mib,
      namespace.wiki_size / {{bytes_to_mib_conversion}}                         AS wiki_mib,
      namespace.storage_size / {{bytes_to_mib_conversion}}                      AS storage_mib,
      repository.free_storage                                                   AS total_free_storage_gib,
      repository.purchased_storage                                              AS total_purchased_storage_gib,
      namespace.billable_storage_size / {{bytes_to_gib_conversion}}             AS billable_storage_gib,
      namespace.repository_size / {{bytes_to_gib_conversion}}                   AS repository_gib,
      namespace.lfs_objects_size / {{bytes_to_gib_conversion}}                  AS lfs_objects_gib,
      namespace.build_artifacts_size / {{bytes_to_gib_conversion}}              AS build_artifacts_gib,
      namespace.packages_size / {{bytes_to_gib_conversion}}                     AS packages_gib,
      namespace.wiki_size / {{bytes_to_gib_conversion}}                         AS wiki_gib,
      namespace.storage_size / {{bytes_to_gib_conversion}}                      AS storage_gib
    FROM namespace_repository_storage_usage_summary repository
    LEFT JOIN top_level_namespace_storage_summary namespace
      ON repository.ultimate_parent_id = namespace.ultimate_parent_id
      AND repository.snapshot_month = namespace.snapshot_month

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-29",
    updated_date="2021-06-17"
) }}