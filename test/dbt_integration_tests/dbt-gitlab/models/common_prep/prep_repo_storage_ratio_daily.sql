WITH final AS (

  SELECT
    prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day,
    COALESCE(prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category, 'internal')  AS finance_pl_category,
    SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb)                          AS repo_size_gb,
    RATIO_TO_REPORT(SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb)) 
      OVER (PARTITION BY prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day)         AS percent_repo_size_gb
  FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
  GROUP BY
    1, 2

)

SELECT *
FROM final