WITH blocked_users AS (

  SELECT
    DATE_TRUNC('month', created_at) AS blocked_month,
    dim_user_id                     AS blocked_user_id
  FROM {{ ref('dim_user') }}
  WHERE is_blocked_user

),

blocked_user_count AS (

  SELECT
    blocked_month,
    COUNT(DISTINCT blocked_user_id) AS blocked_user_count
  FROM blocked_users
  GROUP BY 1

),

blocked_projects AS (

  SELECT
    blocked_users.blocked_month AS blocked_month,
    projects.dim_project_id     AS project_id,
    dim_namespace_id            AS namespace_id
  FROM {{ ref('dim_project') }} AS projects
  INNER JOIN blocked_users ON projects.dim_user_id_creator = blocked_users.blocked_user_id

),

blocked_project_storage_prep AS (

  SELECT
    snapshot_month,
    storage_size / POW(1024, 3)                       AS storage_size_gb,
    project_statistics.project_id,
    repository_size / POW(1024, 3)                    AS repo_size_gb,
    storage_size_gb - repo_size_gb                    AS object_storage_size_gb,
    IFF(snapshot_month <= blocked_month, TRUE, FALSE) AS before_blocked_month
  FROM {{ ref('gitlab_dotcom_project_statistic_monthly_snapshot') }} AS project_statistics
  INNER JOIN blocked_projects ON project_statistics.project_id = blocked_projects.project_id
  WHERE snapshot_month >= '2020-11-01'

),

blocked_project_storage AS (

  SELECT
    snapshot_month,
    'Storage'                   AS abuse_cost_category,
    SUM(repo_size_gb)           AS repo_size_gb,
    SUM(object_storage_size_gb) AS object_storage_size_gb
  FROM blocked_project_storage_prep
  GROUP BY
    1,
    2

),

blocked_project_storage_dt AS (

  SELECT
    snapshot_month,
    'Networking'                                           AS abuse_cost_category,
    SUM(IFF(before_blocked_month, storage_size_gb * 2, 0)) AS cost,-- networking cost = 2* storage size
    SUM(IFF(before_blocked_month, storage_size_gb, 0))     AS data_transfer
  FROM blocked_project_storage_prep
  GROUP BY 1

),

blocked_ci_usage AS (

  SELECT
    DATE_TRUNC('month', builds.started_at)                                                                                                                                                                                                                                                                                                                                                             AS ci_month,
    'CI Compute'                                                                                                                                                                                                                                                                                                                                                                                       AS abuse_cost_category,
    CASE
      WHEN namespaces.namespace_is_internal = TRUE THEN TRUE
      WHEN runners.ci_runner_type = 1 THEN TRUE
      ELSE FALSE
    END                                                                                                                                                                                                                                                                                                                                                                                                AS is_paid_by_gitlab,
    SUM(DATEDIFF(SECOND, builds.started_at, builds.finished_at) / 60)                                                                                                                                                                                                                                                                                                                                  AS ci_minutes,
    SUM(CASE WHEN runners.ci_runner_description LIKE 'shared-runners-manager%' THEN (DATEDIFF(SECOND, builds.started_at, builds.finished_at) / 3600) * .0475 WHEN runners.ci_runner_description LIKE 'windows-shared-runners-manager%' THEN (DATEDIFF(SECOND, builds.started_at, builds.finished_at) / 3600) * .135 ELSE (DATEDIFF(SECOND, builds.started_at, builds.finished_at) / 3600) * .0845 END) AS cost_old,
    /*
    https://gitlab.com/gitlab-org/quality/engineering-analytics/finops-analysis/-/issues/84#note_1518500625
    Unit price is taken from the value of linux small runners in the past 6 months: https://10az.online.tableau.com/#/site/gitlab/views/FinOps-Unit-economics-CICDRunners/UE-Runners-Compute?:iid=2
    */
    SUM(DATEDIFF(SECOND, builds.started_at, builds.finished_at) / 60) / 1000 * 2.43                                                                                                                                                                                                                                                                                                                    AS cost_ue,
    COUNT(DISTINCT builds.dim_ci_build_id)                                                                                                                                                                                                                                                                                                                                                             AS job_count
  FROM {{ ref('dim_ci_runner') }} AS runners
  INNER JOIN {{ ref('dim_ci_build') }} AS builds ON runners.dim_ci_runner_id = builds.dim_ci_runner_id
  INNER JOIN blocked_projects ON builds.dim_project_id = blocked_projects.project_id
  INNER JOIN {{ ref('dim_namespace') }} AS namespaces ON blocked_projects.namespace_id = namespaces.namespace_id
  WHERE builds.started_at >= '2021-01-01'
    AND builds.finished_at IS NOT NULL
    AND is_paid_by_gitlab = TRUE
  {{ dbt_utils.group_by(n=3) }}

),

joined AS (

  SELECT
    blocked_project_storage.snapshot_month                                                                        AS date_month,
    blocked_project_storage.abuse_cost_category,
    /*
    References for Repository costs:
    $173.1TB/month = $0.169GB/month >> taken from Aug/Sep/Oct '23 in https://10az.online.tableau.com/#/site/gitlab/views/FinOps-Unit-economics-Gitaly/UE-Gitaly-Repostorage?:iid=4
    $0.2028/GB for multi-regional Object storage is the contract price instead of listed price
    `0D5D-6E23-4250	Standard Storage US Multi-region`
    */
    blocked_project_storage.repo_size_gb / 1024 * 173.1 + blocked_project_storage.object_storage_size_gb * .02028 AS cost,
    NULL                                                                                                          AS data_transfer,
    'Potential Saving on Storage'                                                                                 AS saving_type
  FROM blocked_project_storage
  UNION ALL
  SELECT
    blocked_ci_usage.ci_month AS date_month,
    blocked_ci_usage.abuse_cost_category,
    blocked_ci_usage.cost_ue  AS cost,
    NULL                      AS data_transfer,
    'Actual Saving for CI'    AS saving_type
  FROM blocked_ci_usage
  UNION ALL
  SELECT
    snapshot_month                 AS date_month,
    abuse_cost_category,
    cost,
    data_transfer,
    'Actual Saving for Networking' AS saving_type
  FROM blocked_project_storage_dt

)

SELECT
  date_month,
  abuse_cost_category,
  cost,
  blocked_user_count,
  data_transfer,
  saving_type
FROM joined
LEFT JOIN blocked_user_count ON joined.date_month = blocked_user_count.blocked_month
