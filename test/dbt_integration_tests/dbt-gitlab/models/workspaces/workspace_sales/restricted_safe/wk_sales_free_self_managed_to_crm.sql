WITH mart_ping_instance_metric_monthly AS (

  SELECT * FROM {{ ref('mart_ping_instance_metric_monthly') }}

-- Sets up a 3-month lookback period from current date for analysis
  WHERE dim_ping_date_id >= TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months'), 'YYYYMMDD')::INT

),

-- instance_mau CTE
-- Purpose: Creates the foundation table with core instance metrics
-- Aggregates key installation identifiers and high-level usage statistics
-- Filters for Free tier, self-managed instances only, excluding example domains
-- Acts as the base table that other metrics will join to
instance_mau AS (

  SELECT
    dim_installation_id,
    MAX(ping_edition_product_tier) AS gitlab_edition,
    MAX(major_minor_version)       AS gitlab_version,
    MIN(ping_created_date_month)   AS first_ping,
    MAX(ping_created_date_month)   AS latest_ping,
    MAX(umau_value)                AS monthly_active_users,
    MAX(host_name)                 AS host_name
  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY ALL

),

-- core_dev CTE
-- Purpose: Aggregates core development activity metrics
-- Calculates key metrics around code commits, merge requests, and CI pipeline usage
-- Tracks developer engagement through various coding activities
-- Provides foundation for measuring development process maturity
core_dev AS (

  SELECT
    dim_installation_id,
    SUM(CASE WHEN metrics_path = 'counts.source_code_pushes' THEN monthly_metric_value ELSE 0 END)                                           AS push_count,
    SUM(CASE WHEN metrics_path = 'counts.merge_requests' THEN monthly_metric_value ELSE 0 END)                                               AS mr_count,
    SUM(CASE WHEN metrics_path = 'counts.ci_builds' THEN monthly_metric_value ELSE 0 END)                                                    AS ci_build_count,
    SUM(CASE WHEN metrics_path = 'counts.ci_pipeline_config_repository' THEN monthly_metric_value ELSE 0 END)                                AS pipeline_config_count,
    SUM(CASE WHEN metrics_path = 'counts.ci_triggers' THEN monthly_metric_value ELSE 0 END)                                                  AS pipeline_trigger_count,
    -- User metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.source_code.git_write_action_monthly' THEN monthly_metric_value ELSE 0 END)             AS monthly_active_developers,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.code_review.i_code_review_user_create_mr_monthly' THEN monthly_metric_value ELSE 0 END) AS monthly_active_mr_creators,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.ide_edit.g_edit_by_web_ide_monthly' THEN monthly_metric_value ELSE 0 END)               AS monthly_active_web_ide_users
  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
  GROUP BY dim_installation_id

),

-- devops CTE
-- Purpose: Measures DevOps lifecycle metrics
-- Tracks deployment frequency, success rates, and environment management
-- Monitors kubernetes integration and release management
-- Helps assess DevOps maturity and automation levels
devops AS (

  SELECT
    dim_installation_id,
    SUM(CASE WHEN metrics_path = 'counts.deployments' THEN monthly_metric_value ELSE 0 END)                                                        AS deployment_count,
    SUM(CASE WHEN metrics_path = 'counts.successful_deployments' THEN monthly_metric_value ELSE 0 END)                                             AS successful_deployment_count,
    SUM(CASE WHEN metrics_path = 'counts.failed_deployments' THEN monthly_metric_value ELSE 0 END)                                                 AS failed_deployment_count,
    SUM(CASE WHEN metrics_path = 'counts.kubernetes_agent_k8s_api_proxy_request' THEN monthly_metric_value ELSE 0 END)                             AS k8s_request_count,
    SUM(CASE WHEN metrics_path = 'counts.releases' THEN monthly_metric_value ELSE 0 END)                                                           AS release_count,
    SUM(CASE WHEN metrics_path = 'counts.environments' THEN monthly_metric_value ELSE 0 END)                                                       AS environment_count,
    -- User metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.ci_users.ci_users_executing_deployment_job_monthly' THEN monthly_metric_value ELSE 0 END)     AS monthly_active_deployers,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.environments.users_visiting_environments_pages_monthly' THEN monthly_metric_value ELSE 0 END) AS monthly_active_environment_users
  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
  GROUP BY dim_installation_id

),

-- security CTE
-- Purpose: Aggregates security and compliance related metrics
-- Tracks usage of various security scanning features (SAST, DAST, container scanning, etc.)
-- Monitors security-focused user activity
-- Used to assess security maturity and compliance adherence
security AS (

  SELECT
    dim_installation_id,
    SUM(CASE WHEN metrics_path = 'counts.sast_jobs' THEN monthly_metric_value ELSE 0 END)                                        AS sast_scan_count,
    SUM(CASE WHEN metrics_path = 'counts.dast_jobs' THEN monthly_metric_value ELSE 0 END)                                        AS dast_scan_count,
    SUM(CASE WHEN metrics_path = 'counts.container_scanning_jobs' THEN monthly_metric_value ELSE 0 END)                          AS container_scan_count,
    SUM(CASE WHEN metrics_path = 'counts.dependency_scanning_jobs' THEN monthly_metric_value ELSE 0 END)                         AS dependency_scan_count,
    SUM(CASE WHEN metrics_path = 'counts.secret_detection_jobs' THEN monthly_metric_value ELSE 0 END)                            AS secret_scan_count,
    SUM(CASE WHEN metrics_path = 'counts.license_management_jobs' THEN monthly_metric_value ELSE 0 END)                          AS license_scan_count,
    SUM(CASE WHEN metrics_path = 'counts_monthly.code_quality_jobs' THEN monthly_metric_value ELSE 0 END)                        AS code_quality_scan_count,
    -- User metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.secure.user_sast_jobs' THEN monthly_metric_value ELSE 0 END)                AS monthly_active_sast_users,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.secure.user_dast_jobs' THEN monthly_metric_value ELSE 0 END)                AS monthly_active_dast_users,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.secure.user_dependency_scanning_jobs' THEN monthly_metric_value ELSE 0 END) AS monthly_active_dependency_scan_users,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.secure.user_container_scanning_jobs' THEN monthly_metric_value ELSE 0 END)  AS monthly_active_container_scan_users,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.secure.user_secret_detection_jobs' THEN monthly_metric_value ELSE 0 END)    AS monthly_active_secret_scan_users
  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
  GROUP BY dim_installation_id

),

-- collab CTE
-- Purpose: Measures collaboration and planning activities
-- Tracks issue management, comments, project organization, and team coordination
-- Monitors user engagement in collaborative features
-- Helps assess team coordination and project management maturity
collab AS (

  SELECT
    dim_installation_id,
    SUM(CASE WHEN metrics_path = 'counts.issues' THEN monthly_metric_value ELSE 0 END)                                                               AS issue_count,
    SUM(CASE WHEN metrics_path = 'counts.notes' THEN monthly_metric_value ELSE 0 END)                                                                AS comment_count,
    SUM(CASE WHEN metrics_path = 'counts.projects' THEN monthly_metric_value ELSE 0 END)                                                             AS project_count,
    SUM(CASE WHEN metrics_path = 'counts.boards' THEN monthly_metric_value ELSE 0 END)                                                               AS board_count,
    SUM(CASE WHEN metrics_path = 'counts.epics' THEN monthly_metric_value ELSE 0 END)                                                                AS epic_count,
    SUM(CASE WHEN metrics_path = 'counts.milestones' THEN monthly_metric_value ELSE 0 END)                                                           AS milestone_count,
    -- User metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.issues_edit.g_project_management_issue_created_monthly' THEN monthly_metric_value ELSE 0 END)   AS monthly_active_issue_creators,
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.code_review.i_code_review_user_review_requested_monthly' THEN monthly_metric_value ELSE 0 END)  AS monthly_active_reviewers,
    MAX(CASE WHEN metrics_path = 'counts_monthly.aggregated_metrics.code_review_category_monthly_active_users' THEN monthly_metric_value ELSE 0 END) AS monthly_code_review_users
  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
  GROUP BY dim_installation_id

),

-- base_metrics CTE
-- Purpose: Combines all previously collected metrics into a single view
-- Joins all previous CTEs (instance_mau, core_dev, devops, security, collab)
-- Creates a comprehensive view of all metrics for each installation
-- Serves as foundation for subsequent scoring and analysis
base_metrics AS (

  SELECT
    i.dim_installation_id,
    i.host_name,
    i.monthly_active_users,
    i.gitlab_edition,
    i.gitlab_version,
    i.first_ping,
    i.latest_ping,

    -- Core Development
    cd.push_count,
    cd.mr_count,
    cd.ci_build_count,
    cd.pipeline_config_count,
    cd.pipeline_trigger_count,
    cd.monthly_active_developers,
    cd.monthly_active_mr_creators,
    cd.monthly_active_web_ide_users,

    -- DevOps Lifecycle
    d.deployment_count,
    d.successful_deployment_count,
    d.failed_deployment_count,
    d.k8s_request_count,
    d.release_count,
    d.environment_count,
    d.monthly_active_deployers,
    d.monthly_active_environment_users,

    -- Security & Compliance
    s.sast_scan_count,
    s.dast_scan_count,
    s.container_scan_count,
    s.dependency_scan_count,
    s.secret_scan_count,
    s.license_scan_count,
    s.code_quality_scan_count,
    s.monthly_active_sast_users,
    s.monthly_active_dast_users,
    s.monthly_active_dependency_scan_users,
    s.monthly_active_container_scan_users,
    s.monthly_active_secret_scan_users,

    -- Collaboration & Planning
    c.issue_count,
    c.comment_count,
    c.project_count,
    c.board_count,
    c.epic_count,
    c.milestone_count,
    c.monthly_active_issue_creators,
    c.monthly_active_reviewers,
    c.monthly_code_review_users
  FROM instance_mau AS i
  LEFT JOIN core_dev AS cd ON i.dim_installation_id = cd.dim_installation_id
  LEFT JOIN devops AS d ON i.dim_installation_id = d.dim_installation_id
  LEFT JOIN security AS s ON i.dim_installation_id = s.dim_installation_id
  LEFT JOIN collab AS c ON i.dim_installation_id = c.dim_installation_id

),

-- core_dev_metrics CTE
-- Purpose: Calculates normalized metrics for core development activities
-- Computes per-developer ratios for key activities
-- Normalizes metrics to account for different team sizes
-- Provides basis for core development scoring
core_dev_metrics AS (

  SELECT
    *,
    CASE
      WHEN monthly_active_developers > 0 THEN mr_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS mrs_per_dev,
    CASE
      WHEN monthly_active_developers > 0 THEN ci_build_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS builds_per_dev,
    CASE
      WHEN monthly_active_developers > 0 THEN pipeline_config_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS pipelines_per_dev
  FROM base_metrics

),

-- core_dev_scores CTE
-- Purpose: Assigns scores to core development metrics
-- Uses percentile rankings to score different aspects of development activity
-- Applies weights to different metrics based on importance
-- Contributes to overall maturity assessment
core_dev_scores AS (

  SELECT
    *,
    CASE
      WHEN mrs_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY mrs_per_dev) * 100
    END * 0.4 AS mr_score,
    CASE
      WHEN builds_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY builds_per_dev) * 100
    END * 0.3 AS build_score,
    CASE
      WHEN pipelines_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY pipelines_per_dev) * 100
    END * 0.2 AS pipeline_score,
    CASE
      WHEN monthly_active_web_ide_users = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY monthly_active_web_ide_users) * 100
    END * 0.1 AS ide_score
  FROM core_dev_metrics

),

-- devops_metrics CTE
-- Purpose: Calculates normalized metrics for DevOps activities
-- Computes per-developer ratios for deployment and environment metrics
-- Adds binary flag for kubernetes usage
-- Provides basis for DevOps maturity scoring
devops_metrics AS (

  SELECT
    *,
    CASE
      WHEN monthly_active_developers > 0 THEN successful_deployment_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS deployments_per_dev,
    CASE
      WHEN monthly_active_developers > 0 THEN environment_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS envs_per_dev,
    CASE
      WHEN monthly_active_developers > 0 THEN release_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS releases_per_dev,
    CASE
      WHEN k8s_request_count > 0 THEN 100
      ELSE 0
    END AS k8s_usage
  FROM core_dev_scores

),

-- devops_scores CTE
-- Purpose: Assigns scores to DevOps metrics
-- Uses percentile rankings to score different aspects of DevOps practices
-- Weights different metrics based on their importance to DevOps maturity
-- Contributes to overall maturity assessment
devops_scores AS (

  SELECT
    *,
    CASE
      WHEN deployments_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY deployments_per_dev) * 100
    END * 0.4       AS deployment_score,
    CASE
      WHEN envs_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY envs_per_dev) * 100
    END * 0.3       AS env_score,
    CASE
      WHEN releases_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY releases_per_dev) * 100
    END * 0.2       AS release_score,
    k8s_usage * 0.1 AS k8s_score
  FROM devops_metrics

),

-- collab_metrics CTE
-- Purpose: Calculates normalized metrics for collaboration activities
-- Computes ratios for review participation and comment activity
-- Measures project management tool adoption
-- Provides basis for collaboration maturity scoring
collab_metrics AS (

  SELECT
    *,
    CASE
      WHEN monthly_active_developers > 0 THEN monthly_active_reviewers::FLOAT / monthly_active_developers
      ELSE 0
    END AS review_rate,
    CASE
      WHEN mr_count > 0 THEN comment_count::FLOAT / mr_count
      ELSE 0
    END AS comments_per_mr,
    CASE
      WHEN monthly_active_developers > 0 THEN issue_count::FLOAT / monthly_active_developers
      ELSE 0
    END AS issues_per_dev,
    CASE
      WHEN project_count > 0 THEN board_count::FLOAT / project_count
      ELSE 0
    END AS board_usage
  FROM devops_scores

),

-- collab_scores CTE
-- Purpose: Assigns scores to collaboration metrics
-- Uses percentile rankings to score different aspects of collaboration
-- Weights metrics based on their importance to team collaboration
-- Contributes to overall maturity assessment
collab_scores AS (

  SELECT
    *,
    CASE
      WHEN review_rate = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY review_rate) * 100
    END * 0.4 AS review_score,
    CASE
      WHEN comments_per_mr = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY comments_per_mr) * 100
    END * 0.3 AS comment_score,
    CASE
      WHEN issues_per_dev = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY issues_per_dev) * 100
    END * 0.2 AS issue_score,
    CASE
      WHEN board_usage = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY board_usage) * 100
    END * 0.1 AS board_score
  FROM collab_metrics

),

-- security_metrics CTE
-- Purpose: Calculates advanced security usage metrics
-- Uses logarithmic scaling for scan volumes to handle wide ranges
-- Measures security feature diversity and coverage
-- Evaluates consistency of security practices
security_metrics AS (

  SELECT
    *,
    -- Calculate total scans with logarithmic scaling
    LN(
      GREATEST(
        1,
        COALESCE(sast_scan_count, 0)
        + COALESCE(dast_scan_count, 0)
        + COALESCE(container_scan_count, 0)
        + COALESCE(dependency_scan_count, 0)
        + COALESCE(secret_scan_count, 0)
        + COALESCE(license_scan_count, 0)
      )
    )        AS ln_total_scans,

    -- Calculate scan diversity (0-6 scale)
    (
      CASE WHEN COALESCE(sast_scan_count, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(dast_scan_count, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(container_scan_count, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(dependency_scan_count, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(secret_scan_count, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(license_scan_count, 0) > 0 THEN 1 ELSE 0 END
    )::FLOAT AS security_feature_adoption,

    -- Calculate security coverage (scans per MR with diminishing returns)
    CASE
      WHEN mr_count > 0
        THEN
          LEAST(1.0, (
            COALESCE(sast_scan_count, 0)
            + COALESCE(dast_scan_count, 0)
            + COALESCE(container_scan_count, 0)
            + COALESCE(dependency_scan_count, 0)
            + COALESCE(secret_scan_count, 0)
            + COALESCE(license_scan_count, 0)
          )::FLOAT / (mr_count * 3))
      ELSE 0
    END      AS scan_coverage,

    -- Calculate user adoption with logarithmic scaling
    LN(
      GREATEST(
        1,
        COALESCE(monthly_active_sast_users, 0)
        + COALESCE(monthly_active_dast_users, 0)
        + COALESCE(monthly_active_dependency_scan_users, 0)
        + COALESCE(monthly_active_container_scan_users, 0)
        + COALESCE(monthly_active_secret_scan_users, 0)
      )
    )        AS ln_security_users,

    -- Track consistency of scanning
    CASE
      WHEN mr_count >= 10 AND (
          COALESCE(sast_scan_count, 0)
          + COALESCE(dast_scan_count, 0)
          + COALESCE(container_scan_count, 0)
          + COALESCE(dependency_scan_count, 0)
          + COALESCE(secret_scan_count, 0)
        ) >= mr_count * 0.8 THEN 1
      ELSE 0
    END      AS consistent_scanning
  FROM collab_scores

),

-- security_scores CTE
-- Purpose: Assigns scores to security metrics
-- Uses sophisticated scoring approach including volume, coverage, and consistency
-- Weights different aspects of security practices
-- Contributes significantly to overall maturity assessment
security_scores AS (

  SELECT
    *,
    -- Volume score (25%) - Rewards high scanning volume with diminishing returns
    CASE
      WHEN ln_total_scans = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY ln_total_scans) * 100
    END * 0.25                                   AS volume_score,

    -- Coverage score (25%) - Rewards consistent scanning across MRs
    (scan_coverage * 100) * 0.25                 AS coverage_score,

    -- Feature adoption score (25%) - Rewards using multiple security features
    (security_feature_adoption / 6 * 100) * 0.25 AS feature_score,

    -- User adoption score (15%) - Rewards having more security-aware users
    CASE
      WHEN ln_security_users = 0 THEN 0
      ELSE PERCENT_RANK() OVER (ORDER BY ln_security_users) * 100
    END * 0.15                                   AS user_score,

    -- Consistency bonus (10%) - Rewards maintaining high security standards
    (consistent_scanning * 100) * 0.10           AS consistency_score

  FROM security_metrics

),

-- final_scores CTE
-- Purpose: Aggregates all category scores into final scoring components
-- Combines scores from core development, DevOps, collaboration, and security
-- Maintains original weighting for each category
-- Prepares data for final maturity calculation
final_scores AS (

  SELECT
    *,
    -- Core Development (35% of total)
    (mr_score + build_score + pipeline_score + ide_score)                            AS core_score,

    -- DevOps (25% of total)
    (deployment_score + env_score + release_score + k8s_score)                       AS devops_score,

    -- Collaboration (25% of total)
    (review_score + comment_score + issue_score + board_score)                       AS collab_score,

    -- Security (15% of total) - Using new methodology
    (volume_score + coverage_score + feature_score + user_score + consistency_score) AS security_score
  FROM security_scores

),

-- weighted_scores CTE
-- Purpose: Calculates final instance maturity score and determines maturity level
-- Applies category weights to get total score
-- Calculates percentile thresholds for maturity level determination
-- Establishes the final maturity classification
weighted_scores AS (

  SELECT
    *,
    -- Apply category weights to get total score
    (
      core_score * 0.35      -- Core Development (35%)
      + devops_score * 0.25    -- DevOps (25%)
      + collab_score * 0.25    -- Collaboration (25%)
      + security_score * 0.15     -- Security (15%)
    )         AS total_score,

    -- Calculate percentile thresholds for maturity levels
    PERCENTILE_CONT(0.20) WITHIN GROUP (
      ORDER BY
        (
          core_score * 0.35
          + devops_score * 0.25
          + collab_score * 0.25
          + security_score * 0.15
        )
    ) OVER () AS p20,

    PERCENTILE_CONT(0.55) WITHIN GROUP (
      ORDER BY
        (
          core_score * 0.35
          + devops_score * 0.25
          + collab_score * 0.25
          + security_score * 0.15
        )
    ) OVER () AS p55,

    PERCENTILE_CONT(0.80) WITHIN GROUP (
      ORDER BY
        (
          core_score * 0.35
          + devops_score * 0.25
          + collab_score * 0.25
          + security_score * 0.15
        )
    ) OVER () AS p80,

    PERCENTILE_CONT(0.95) WITHIN GROUP (
      ORDER BY
        (
          core_score * 0.35
          + devops_score * 0.25
          + collab_score * 0.25
          + security_score * 0.15
        )
    ) OVER () AS p95
  FROM final_scores

),

-- security_metrics_for_use_summary CTE
-- Purpose: Gathers detailed security metrics for usage pattern analysis
-- Collects metrics on different types of security scanning activities
-- Tracks compliance controls and security review processes
-- Used to identify security-focused usage patterns
security_metrics_for_use_summary AS (

  SELECT
    dim_installation_id,
    -- Security scanning metrics
    MAX(CASE WHEN metrics_path = 'counts.secret_detection_jobs'
        THEN monthly_metric_value
      ELSE 0
    END) AS secret_detection_jobs,
    MAX(CASE WHEN metrics_path = 'counts.sast_jobs'
        THEN monthly_metric_value
      ELSE 0
    END) AS sast_jobs,
    MAX(CASE WHEN metrics_path = 'counts.container_scanning_jobs'
        THEN monthly_metric_value
      ELSE 0
    END) AS container_scanning_jobs,
    MAX(CASE WHEN metrics_path = 'counts.dependency_scanning_jobs'
        THEN monthly_metric_value
      ELSE 0
    END) AS dependency_scanning_jobs,

    -- Compliance controls metrics
    MAX(CASE WHEN metrics_path = 'counts.merged_merge_requests_using_approval_rules'
        THEN monthly_metric_value
      ELSE 0
    END) AS mr_with_approval_rules,
    MAX(CASE WHEN metrics_path = 'counts.protected_branches'
        THEN monthly_metric_value
      ELSE 0
    END) AS protected_branches,

    -- Security review metrics
    MAX(CASE WHEN metrics_path = 'counts.i_code_review_merge_request_widget_security_reports_count_view'
        THEN monthly_metric_value
      ELSE 0
    END) AS security_report_views,

    -- Additional security indicators
    MAX(CASE WHEN metrics_path = 'counts.projects_sonarqube_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS sonarqube_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_with_repositories_enabled'
        THEN monthly_metric_value
      ELSE 0
    END) AS repos_with_security

  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY dim_installation_id

),

-- security_output_for_use_summary CTE
-- Purpose: Analyzes security usage patterns and identifies high-complexity scenarios
-- Creates flags for different levels of security tool usage and complexity
-- Identifies instances with sophisticated security needs
-- Contributes to overall usage pattern analysis
security_output_for_use_summary AS (

  SELECT
    dim_installation_id,

    -- Security scanning volume flags
    CASE WHEN (
        secret_detection_jobs + sast_jobs
        + container_scanning_jobs + dependency_scanning_jobs
      ) > 500
        THEN 1
      ELSE 0
    END AS is_critical_security_volume,

    CASE WHEN (
        secret_detection_jobs + sast_jobs
        + container_scanning_jobs + dependency_scanning_jobs
      ) BETWEEN 100 AND 499
        THEN 1
      ELSE 0
    END AS is_high_security_volume,

    -- Security scanner diversity flag
    CASE WHEN (
        CASE WHEN secret_detection_jobs > 100 THEN 1 ELSE 0 END
        + CASE WHEN sast_jobs > 100 THEN 1 ELSE 0 END
        + CASE WHEN container_scanning_jobs > 100 THEN 1 ELSE 0 END
        + CASE WHEN dependency_scanning_jobs > 100 THEN 1 ELSE 0 END
      ) >= 3
        THEN 1
      ELSE 0
    END AS has_diverse_security_tools,

    -- Compliance feature usage flag
    CASE WHEN (
        CASE WHEN mr_with_approval_rules > 50 THEN 1 ELSE 0 END
        + CASE WHEN protected_branches > 10 THEN 1 ELSE 0 END
        + CASE WHEN sonarqube_active > 0 THEN 1 ELSE 0 END
      ) >= 3
        THEN 1
      ELSE 0
    END AS has_high_compliance_needs,

    -- Security review activity flags
    CASE WHEN security_report_views > 200
        THEN 1
      ELSE 0
    END AS has_critical_security_reviews,

    CASE WHEN security_report_views BETWEEN 50 AND 199
        THEN 1
      ELSE 0
    END AS has_high_security_reviews

  FROM security_metrics_for_use_summary

),

-- pipeline_metrics_for_use_summary CTE
-- Purpose: Gathers detailed pipeline and CI/CD metrics for usage pattern analysis
-- Tracks pipeline volume, complexity, runner infrastructure, and deployment patterns
-- Monitors multi-project pipeline configurations and webhook usage
-- Used to identify sophisticated CI/CD usage patterns
pipeline_metrics_for_use_summary AS (
 
  SELECT
    dim_installation_id,
    -- Pipeline volume metrics
    MAX(CASE WHEN metrics_path = 'counts.ci_internal_pipelines'
        THEN monthly_metric_value
      ELSE 0
    END) AS internal_pipelines,
    MAX(CASE WHEN metrics_path = 'counts.ci_builds'
        THEN monthly_metric_value
      ELSE 0
    END) AS ci_builds,
    MAX(CASE WHEN metrics_path = 'counts.ci_external_pipelines'
        THEN monthly_metric_value
      ELSE 0
    END) AS external_pipelines,

    -- Multi-project complexity metrics
    MAX(CASE WHEN metrics_path = 'counts.ci_triggers'
        THEN monthly_metric_value
      ELSE 0
    END) AS ci_triggers,
    MAX(CASE WHEN metrics_path = 'counts.web_hooks'
        THEN monthly_metric_value
      ELSE 0
    END) AS webhooks,

    -- Runner infrastructure metrics
    MAX(CASE WHEN metrics_path = 'counts.ci_runners_project_type_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS project_runners,
    MAX(CASE WHEN metrics_path = 'counts.ci_runners_instance_type_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS instance_runners,

    -- Deployment metrics
    MAX(CASE WHEN metrics_path = 'counts.environments'
        THEN monthly_metric_value
      ELSE 0
    END) AS environment_count,
    MAX(CASE WHEN metrics_path = 'counts.successful_deployments'
        THEN monthly_metric_value
      ELSE 0
    END) AS successful_deployments,

    -- Configuration complexity indicators
    MAX(CASE WHEN metrics_path = 'counts.ci_pipeline_config_repository'
        THEN monthly_metric_value
      ELSE 0
    END) AS pipeline_configs,
    MAX(CASE WHEN metrics_path = 'counts.ci_pipeline_schedules'
        THEN monthly_metric_value
      ELSE 0
    END) AS pipeline_schedules

  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY dim_installation_id

),

-- pipeline_output_for_use_summary CTE
-- Purpose: Analyzes pipeline usage patterns and identifies high-complexity scenarios
-- Creates flags for different levels of pipeline complexity and scale
-- Evaluates deployment complexity and runner infrastructure needs
-- Contributes to overall usage pattern analysis for CI/CD features
pipeline_output_for_use_summary AS (

  SELECT
    dim_installation_id,

    -- Pipeline volume flags
    CASE WHEN internal_pipelines > 5000 OR ci_builds > 25000
        THEN 1
      ELSE 0
    END AS is_critical_pipeline_volume,

    CASE WHEN internal_pipelines > 1000 OR ci_builds > 5000
        THEN 1
      ELSE 0
    END AS is_high_pipeline_volume,

    -- Multi-project complexity flag
    CASE WHEN ci_triggers > 50 AND webhooks > 10
        THEN 1
      ELSE 0
    END AS is_complex_project_integration,

    -- Runner infrastructure flags
    CASE WHEN (project_runners + instance_runners) > 50
        THEN 1
      ELSE 0
    END AS is_critical_runner_scale,

    CASE WHEN (project_runners + instance_runners) > 20
        THEN 1
      ELSE 0
    END AS is_high_runner_scale,

    -- Deployment complexity flag
    CASE WHEN environment_count > 10
        THEN 1
      ELSE 0
    END AS is_critical_deployment_complexity,

    -- Overall complexity score
    CASE WHEN (
        -- Pipeline volume score
        CASE
          WHEN internal_pipelines > 5000 OR ci_builds > 25000 THEN 3
          WHEN internal_pipelines > 1000 OR ci_builds > 5000 THEN 2
          WHEN internal_pipelines > 500 OR ci_builds > 2500 THEN 1
          ELSE 0
        END
        -- Project complexity score
        + CASE
          WHEN ci_triggers > 50 AND webhooks > 10 THEN 2
          WHEN ci_triggers > 20 OR webhooks > 5 THEN 1
          ELSE 0
        END
        -- Runner scale score
        + CASE
          WHEN (project_runners + instance_runners) > 50 THEN 3
          WHEN (project_runners + instance_runners) > 20 THEN 2
          WHEN (project_runners + instance_runners) > 10 THEN 1
          ELSE 0
        END
        -- Deployment complexity score
        + CASE
          WHEN environment_count > 10 THEN 3
          WHEN environment_count > 5 THEN 2
          WHEN environment_count > 3 THEN 1
          ELSE 0
        END
      ) >= 8 THEN 1
      ELSE 0
    END AS is_critical_complexity

  FROM pipeline_metrics_for_use_summary

),

-- collaboration_input_for_use_summary CTE
-- Purpose: Gathers detailed collaboration and user activity metrics
-- Tracks code review participation, project management activities, and team interactions
-- Monitors user engagement across different collaboration features
-- Used to identify patterns of team coordination and project organization
collaboration_input_for_use_summary AS (

  SELECT
    dim_installation_id,
    -- User activity metrics
    MAX(CASE WHEN metrics_path = 'counts.notes'
        THEN monthly_metric_value
      ELSE 0
    END) AS total_notes,
    MAX(CASE WHEN metrics_path = 'counts.merge_requests'
        THEN monthly_metric_value
      ELSE 0
    END) AS total_merge_requests,

    -- Code review metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.code_review.i_code_review_user_reviewers_changed_monthly'
        THEN monthly_metric_value
      ELSE 0
    END) AS unique_reviewers,
    MAX(CASE WHEN metrics_path = 'counts.projects_with_repositories_enabled'
        THEN monthly_metric_value
      ELSE 0
    END) AS active_projects,
    MAX(CASE WHEN metrics_path = 'counts.protected_branches'
        THEN monthly_metric_value
      ELSE 0
    END) AS protected_branches,

    -- Team collaboration metrics
    MAX(CASE WHEN metrics_path = 'counts.merge_requests_with_approval_rules'
        THEN monthly_metric_value
      ELSE 0
    END) AS mr_with_approval_rules,
    MAX(CASE WHEN metrics_path = 'counts.unique_users_all'
        THEN monthly_metric_value
      ELSE 0
    END) AS monthly_active_users,

    -- Review complexity metrics
    MAX(CASE WHEN metrics_path = 'counts.open_merge_requests'
        THEN monthly_metric_value
      ELSE 0
    END) AS open_merge_requests,
    MAX(CASE WHEN metrics_path = 'counts.merged_merge_requests'
        THEN monthly_metric_value
      ELSE 0
    END) AS merged_merge_requests

  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY dim_installation_id

),

-- collaboration_output_for_use_summary CTE
-- Purpose: Analyzes collaboration patterns and identifies high-engagement scenarios
-- Creates flags for different levels of user activity and review complexity
-- Evaluates project scale and team coordination patterns
-- Helps identify instances with sophisticated collaboration needs
collaboration_output_for_use_summary AS (

  SELECT
    dim_installation_id,

    -- User activity flags
    CASE WHEN monthly_active_users > 100 OR total_notes > 5000
        THEN 1
      ELSE 0
    END AS is_critical_user_activity,

    CASE WHEN monthly_active_users > 50 OR total_notes > 1000
        THEN 1
      ELSE 0
    END AS is_high_user_activity,

    -- Code review complexity flag
    CASE WHEN open_merge_requests > 50 AND unique_reviewers > 100
        THEN 1
      ELSE 0
    END AS is_critical_review_complexity,

    -- Project scale flags
    CASE WHEN active_projects > 100
        THEN 1
      ELSE 0
    END AS is_critical_project_scale,

    CASE WHEN active_projects > 50
        THEN 1
      ELSE 0
    END AS is_high_project_scale,

    -- Team collaboration flag
    CASE WHEN mr_with_approval_rules > 100 AND unique_reviewers > 50
        THEN 1
      ELSE 0
    END AS is_complex_collaboration,

    -- Overall complexity score based on multiple factors
    CASE WHEN (
        -- User volume score
        CASE
          WHEN monthly_active_users > 100 THEN 3
          WHEN monthly_active_users > 50 THEN 2
          WHEN monthly_active_users > 20 THEN 1
          ELSE 0
        END
        -- Review complexity score
        + CASE
          WHEN open_merge_requests > 50 THEN 3
          WHEN open_merge_requests > 20 THEN 2
          WHEN open_merge_requests > 10 THEN 1
          ELSE 0
        END
        -- Project scale score
        + CASE
          WHEN active_projects > 100 THEN 3
          WHEN active_projects > 50 THEN 2
          WHEN active_projects > 20 THEN 1
          ELSE 0
        END
        -- Communication volume score
        + CASE
          WHEN total_notes > 5000 THEN 3
          WHEN total_notes > 1000 THEN 2
          WHEN total_notes > 500 THEN 1
          ELSE 0
        END
      ) >= 8 THEN 1
      ELSE 0
    END AS is_critical_complexity

  FROM collaboration_input_for_use_summary

),

-- availability_metrics_for_use_summary CTE
-- Purpose: Gathers metrics related to high availability and deployment reliability
-- Tracks deployment success rates, environment complexity, and infrastructure integration
-- Monitors business-critical feature usage and deployment patterns
-- Used to identify instances requiring enterprise-grade availability
availability_metrics_for_use_summary AS (

  SELECT
    dim_installation_id,
    -- Deployment metrics
    MAX(CASE WHEN metrics_path = 'counts.deployments'
        THEN monthly_metric_value
      ELSE 0
    END) AS total_deployments,
    MAX(CASE WHEN metrics_path = 'counts.successful_deployments'
        THEN monthly_metric_value
      ELSE 0
    END) AS successful_deployments,
    MAX(CASE WHEN metrics_path = 'counts.failed_deployments'
        THEN monthly_metric_value
      ELSE 0
    END) AS failed_deployments,

    -- Environment and cluster metrics
    MAX(CASE WHEN metrics_path = 'counts.environments'
        THEN monthly_metric_value
      ELSE 0
    END) AS environment_count,
    MAX(CASE WHEN metrics_path = 'counts.clusters'
        THEN monthly_metric_value
      ELSE 0
    END) AS cluster_count,
    MAX(CASE WHEN metrics_path = 'counts.clusters_enabled'
        THEN monthly_metric_value
      ELSE 0
    END) AS enabled_clusters,

    -- Infrastructure integration metrics
    MAX(CASE WHEN metrics_path = 'counts.clusters_applications_prometheus'
        THEN monthly_metric_value
      ELSE 0
    END) AS prometheus_enabled,
    MAX(CASE WHEN metrics_path = 'counts.clusters_applications_runner'
        THEN monthly_metric_value
      ELSE 0
    END) AS runner_enabled,

    -- Usage pattern metrics
    MAX(CASE WHEN metrics_path = 'redis_hll_counters.ci_users.ci_users_executing_deployment_job_monthly'
        THEN monthly_metric_value
      ELSE 0
    END) AS active_deployers,

    -- Business critical features
    MAX(CASE WHEN metrics_path = 'counts.terraform_states'
        THEN monthly_metric_value
      ELSE 0
    END) AS terraform_states,
    MAX(CASE WHEN metrics_path = 'counts.kubernetes_agents'
        THEN monthly_metric_value
      ELSE 0
    END) AS k8s_agents

  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY dim_installation_id

),

-- availability_output_for_use_summary CTE
-- Purpose: Analyzes availability patterns and identifies high-reliability needs
-- Creates flags for deployment reliability risks and infrastructure complexity
-- Evaluates high-availability readiness and business-critical usage
-- Helps identify instances requiring enterprise-grade availability features
availability_output_for_use_summary AS (

  SELECT
    dim_installation_id,

    -- Deployment reliability flag - Identifies instances with potential stability challenges
    CASE WHEN total_deployments > 100
        AND (CASE
          WHEN total_deployments > 0
            THEN (failed_deployments * 100.0 / total_deployments)
          ELSE 0
        END) > 10
        THEN 1
      ELSE 0
    END AS is_reliability_risk,

    -- Complex infrastructure flag - Identifies sophisticated deployment architectures
    CASE WHEN environment_count > 5 AND cluster_count > 1
        THEN 1
      ELSE 0
    END AS is_complex_infrastructure,

    -- High availability readiness flag - Identifies instances prepared for HA setup
    CASE WHEN prometheus_enabled > 0
        AND runner_enabled > 0
        AND active_deployers > 20
        THEN 1
      ELSE 0
    END AS is_ha_ready,

    -- Business critical usage flag - Identifies mission-critical installations
    CASE WHEN terraform_states > 0
        AND k8s_agents > 0
        AND total_deployments > 100
        THEN 1
      ELSE 0
    END AS is_business_critical,

    -- Critical risk combination flag - Identifies instances needing immediate attention
    CASE WHEN total_deployments > 100
        AND environment_count > 5
        AND (CASE
          WHEN total_deployments > 0
            THEN (failed_deployments * 100.0 / total_deployments)
          ELSE 0
        END) > 10
        THEN 1
      ELSE 0
    END AS is_critical_risk,

    -- Overall high availability needs assessment
    CASE WHEN (
        -- Deployment frequency score
        CASE
          WHEN total_deployments > 500 THEN 3
          WHEN total_deployments > 100 THEN 2
          WHEN total_deployments > 50 THEN 1
          ELSE 0
        END
        -- Environment complexity score
        + CASE
          WHEN environment_count > 10 THEN 3
          WHEN environment_count > 5 THEN 2
          WHEN environment_count > 3 THEN 1
          ELSE 0
        END
        -- Infrastructure integration score
        + CASE
          WHEN (prometheus_enabled > 0 AND runner_enabled > 0) THEN 2
          WHEN (prometheus_enabled > 0 OR runner_enabled > 0) THEN 1
          ELSE 0
        END
        -- Usage intensity score
        + CASE
          WHEN active_deployers > 50 THEN 3
          WHEN active_deployers > 20 THEN 2
          WHEN active_deployers > 10 THEN 1
          ELSE 0
        END
        -- Business critical score
        + CASE
          WHEN (terraform_states > 0 AND k8s_agents > 0) THEN 2
          WHEN (terraform_states > 0 OR k8s_agents > 0) THEN 1
          ELSE 0
        END
      ) >= 10 THEN 1
      ELSE 0
    END AS is_critical_ha_needs

  FROM availability_metrics_for_use_summary

),

-- integration_metrics_for_use_summary CTE
-- Purpose: Gathers metrics about external tool integration patterns
-- Tracks integration with CI/CD tools, package registries, and communication platforms
-- Monitors usage of project management integrations
-- Used to identify complex multi-tool environments and integration needs
integration_metrics_for_use_summary AS (

  SELECT
    dim_installation_id,
    -- Integration volume metrics
    MAX(CASE WHEN metrics_path = 'counts.web_hooks'
        THEN monthly_metric_value
      ELSE 0
    END) AS webhook_count,

    -- CI/CD tool metrics - Track external CI tool usage
    MAX(CASE WHEN metrics_path = 'counts.projects_jenkins_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS jenkins_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_bamboo_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS bamboo_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_teamcity_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS teamcity_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_drone_ci_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS droneci_active,

    -- Package registry metrics - Track artifact management diversity
    MAX(CASE WHEN metrics_path = 'counts.package_events_i_package_npm_pull_package'
        THEN monthly_metric_value
      ELSE 0
    END) AS npm_packages,
    MAX(CASE WHEN metrics_path = 'counts.package_events_i_package_maven_pull_package'
        THEN monthly_metric_value
      ELSE 0
    END) AS maven_packages,
    MAX(CASE WHEN metrics_path = 'counts.package_events_i_package_pypi_pull_package'
        THEN monthly_metric_value
      ELSE 0
    END) AS pypi_packages,
    MAX(CASE WHEN metrics_path = 'counts.package_events_i_container_registry_pull_package'
        THEN monthly_metric_value
      ELSE 0
    END) AS container_images,
    MAX(CASE WHEN metrics_path = 'counts.package_events_i_package_helm_pull_chart'
        THEN monthly_metric_value
      ELSE 0
    END) AS helm_charts,

    -- Communication tool metrics - Track messaging platform integration
    MAX(CASE WHEN metrics_path = 'counts.projects_slack_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS slack_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_microsoft_teams_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS teams_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_discord_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS discord_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_mattermost_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS mattermost_active,

    -- Project management tool metrics - Track PM tool integration
    MAX(CASE WHEN metrics_path = 'counts.projects_jira_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS jira_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_youtrack_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS youtrack_active,
    MAX(CASE WHEN metrics_path = 'counts.projects_asana_active'
        THEN monthly_metric_value
      ELSE 0
    END) AS asana_active

  FROM mart_ping_instance_metric_monthly
  WHERE ping_product_tier = 'Free'
    AND ping_deployment_type = 'Self-Managed'
    AND host_name <> 'gitlab.example.com'
  GROUP BY dim_installation_id

),

-- integration_output_for_use_summary CTE
-- Purpose: Analyzes integration patterns and identifies tool sprawl scenarios
-- Creates flags for different types of integration complexity
-- Evaluates diversity of tool usage across different categories
-- Helps identify instances with complex integration needs
integration_output_for_use_summary AS (

  SELECT
    dim_installation_id,

    -- High integration volume flag - Identifies instances with complex system interconnections
    CASE WHEN webhook_count > 10
        THEN 1
      ELSE 0
    END AS is_high_integration_volume,

    -- CI/CD fragmentation flag - Identifies instances managing multiple CI/CD tools
    CASE WHEN (
        CASE WHEN jenkins_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN bamboo_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN teamcity_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN droneci_active > 0 THEN 1 ELSE 0 END
      ) >= 3
        THEN 1
      ELSE 0
    END AS is_high_cicd_fragmentation,

    -- Package diversity flag - Identifies instances with diverse technology stacks
    CASE WHEN (
        CASE WHEN npm_packages > 0 THEN 1 ELSE 0 END
        + CASE WHEN maven_packages > 0 THEN 1 ELSE 0 END
        + CASE WHEN pypi_packages > 0 THEN 1 ELSE 0 END
        + CASE WHEN container_images > 0 THEN 1 ELSE 0 END
        + CASE WHEN helm_charts > 0 THEN 1 ELSE 0 END
      ) >= 3
        THEN 1
      ELSE 0
    END AS is_high_package_diversity,

    -- Messaging tool overlap flag - Identifies communication tool fragmentation
    CASE WHEN (
        CASE WHEN slack_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN teams_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN discord_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN mattermost_active > 0 THEN 1 ELSE 0 END
      ) >= 3
        THEN 1
      ELSE 0
    END AS is_high_messaging_overlap,

    -- Project management tool overlap flag - Identifies PM tool fragmentation
    CASE WHEN (
        CASE WHEN jira_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN youtrack_active > 0 THEN 1 ELSE 0 END
        + CASE WHEN asana_active > 0 THEN 1 ELSE 0 END
      ) >= 2
        THEN 1
      ELSE 0
    END AS is_high_pm_overlap,

    -- Critical sprawl evaluation - Comprehensive assessment of tool sprawl
    CASE WHEN (
        -- Webhook complexity score
        CASE
          WHEN webhook_count > 20 THEN 3
          WHEN webhook_count > 10 THEN 2
          WHEN webhook_count > 5 THEN 1
          ELSE 0
        END
        -- CI/CD tool diversity score
        + CASE
          WHEN (jenkins_active + bamboo_active + teamcity_active + droneci_active) >= 5 THEN 3
          WHEN (jenkins_active + bamboo_active + teamcity_active + droneci_active) >= 3 THEN 2
          WHEN (jenkins_active + bamboo_active + teamcity_active + droneci_active) >= 1 THEN 1
          ELSE 0
        END
        -- Package registry diversity score
        + CASE
          WHEN (npm_packages + maven_packages + pypi_packages + container_images + helm_charts) >= 5 THEN 3
          WHEN (npm_packages + maven_packages + pypi_packages + container_images + helm_charts) >= 3 THEN 2
          WHEN (npm_packages + maven_packages + pypi_packages + container_images + helm_charts) >= 1 THEN 1
          ELSE 0
        END
        -- Communication tool fragmentation score
        + CASE
          WHEN (slack_active + teams_active + discord_active + mattermost_active) >= 4 THEN 3
          WHEN (slack_active + teams_active + discord_active + mattermost_active) >= 2 THEN 2
          WHEN (slack_active + teams_active + discord_active + mattermost_active) >= 1 THEN 1
          ELSE 0
        END
        -- PM tool fragmentation score
        + CASE
          WHEN (jira_active + youtrack_active + asana_active) >= 3 THEN 3
          WHEN (jira_active + youtrack_active + asana_active) >= 2 THEN 2
          WHEN (jira_active + youtrack_active + asana_active) >= 1 THEN 1
          ELSE 0
        END
      ) >= 10 THEN 1
      ELSE 0
    END AS is_critical_sprawl,

    -- Tool count reference metrics for detailed analysis
    (
      CASE WHEN jenkins_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN bamboo_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN teamcity_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN droneci_active > 0 THEN 1 ELSE 0 END
    )   AS active_cicd_tools,

    (
      CASE WHEN npm_packages > 0 THEN 1 ELSE 0 END
      + CASE WHEN maven_packages > 0 THEN 1 ELSE 0 END
      + CASE WHEN pypi_packages > 0 THEN 1 ELSE 0 END
      + CASE WHEN container_images > 0 THEN 1 ELSE 0 END
      + CASE WHEN helm_charts > 0 THEN 1 ELSE 0 END
    )   AS package_types_count,

    (
      CASE WHEN slack_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN teams_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN discord_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN mattermost_active > 0 THEN 1 ELSE 0 END
    )   AS messaging_integrations,

    (
      CASE WHEN jira_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN youtrack_active > 0 THEN 1 ELSE 0 END
      + CASE WHEN asana_active > 0 THEN 1 ELSE 0 END
    )   AS pm_integrations

  FROM integration_metrics_for_use_summary

),

-- product_usage_summary_aggregation CTE
-- Purpose: The final boss! Combines all usage patterns into a comprehensive analysis
-- Aggregates flags and indicators from all previous analysis CTEs
-- Generates detailed usage descriptions based on observed patterns
-- Provides the ultimate insight into instance complexity and maturity
product_usage_summary_aggregation AS (

  SELECT
    -- Pull in all security metrics for comprehensive analysis
    a.dim_installation_id,
    a.is_critical_security_volume,     -- Identifies extreme security scanning usage
    a.is_high_security_volume,         -- Identifies significant security tool adoption
    a.has_diverse_security_tools,      -- Shows sophisticated security approach
    a.has_high_compliance_needs,       -- Indicates regulatory/compliance focus
    a.has_critical_security_reviews,   -- Shows intensive security review processes
    a.has_high_security_reviews,       -- Indicates active security monitoring

    -- Pipeline complexity indicators
    b.is_critical_pipeline_volume,     -- Identifies extreme CI/CD automation
    b.is_high_pipeline_volume,         -- Shows significant pipeline usage
    b.is_complex_project_integration,  -- Indicates sophisticated project interconnections
    b.is_critical_runner_scale,        -- Shows large-scale build infrastructure
    b.is_high_runner_scale,            -- Indicates significant runner needs
    b.is_critical_deployment_complexity, -- Shows complex deployment patterns
    b.is_critical_complexity
      AS is_critical_complexity_pipeline, -- Overall pipeline complexity flag

    -- Collaboration and team metrics
    c.is_critical_user_activity,       -- Identifies highly active user bases
    c.is_high_user_activity,          -- Shows significant team engagement
    c.is_critical_review_complexity,   -- Indicates sophisticated review processes
    c.is_critical_project_scale,       -- Shows large-scale project management
    c.is_complex_collaboration,        -- Indicates formal collaboration processes
    c.is_critical_complexity
      AS is_critical_complexity_collab, -- Overall collaboration complexity flag

    -- High availability and reliability indicators
    d.is_reliability_risk,            -- Flags potential stability concerns
    d.is_complex_infrastructure,      -- Shows sophisticated deployment architecture
    d.is_ha_ready,                    -- Indicates readiness for high availability
    d.is_business_critical,           -- Shows mission-critical usage
    d.is_critical_risk,               -- Flags urgent reliability needs
    d.is_critical_ha_needs,           -- Overall high availability needs flag

    -- Integration and tool usage patterns
    e.is_high_integration_volume,     -- Shows complex system interconnections
    e.is_high_cicd_fragmentation,     -- Indicates multiple CI/CD tool usage
    e.is_high_package_diversity,      -- Shows diverse technology stack
    e.is_high_messaging_overlap,      -- Indicates communication tool sprawl
    e.is_high_pm_overlap,            -- Shows project management tool diversity
    e.is_critical_sprawl,            -- Overall tool sprawl indicator

    -- Dynamic usage pattern description - The crown jewel of our analysis!
    COALESCE(
      -- Start with user activity assessment
      CASE
        WHEN c.is_critical_user_activity = 1
          THEN 'This GitLab instance has over 100 monthly active users or 5,000+ comments/notes, indicating very high user engagement and collaboration. '
        WHEN c.is_high_user_activity = 1
          THEN 'This GitLab instance has 50 or more monthly active users or 1,000 or more comments/notes, showing significant team activity. '
        ELSE ''
      END

      -- Add pipeline volume assessment
      || CASE
        WHEN b.is_critical_pipeline_volume = 1
          THEN 'This GitLab instance runs over 5,000 internal pipelines or 25,000 CI builds. This instance has extremely high CI/CD automation needs. '
        WHEN b.is_high_pipeline_volume = 1
          THEN 'This GitLab instance runs over 1,000 internal pipelines or 5,000 CI builds, showing significant but not extreme CI/CD usage. '
        ELSE ''
      END

      -- Include runner infrastructure details
      || CASE
        WHEN b.is_critical_runner_scale = 1
          THEN 'This GitLab instance manages more than 50 total runners (both project-specific and instance-wide), suggesting a large-scale build infrastructure. '
        WHEN b.is_high_runner_scale = 1
          THEN 'This GitLab instance has 20-49 total runners, indicating substantial but not extreme build infrastructure needs. '
        ELSE ''
      END

      -- Add security volume assessment
      || CASE
        WHEN a.is_critical_security_volume = 1
          THEN 'This GitLab instance runs over 500 total security jobs, combining secret detection, SAST, container scanning, and dependency scanning. This instance is one of the most security-intensive users. '
        WHEN a.is_high_security_volume = 1
          THEN 'This GitLab instance performs between 100-499 total security jobs, indicating significant but not intensive security scanning activity. '
        ELSE ''
      END

      -- Include security review practices
      || CASE
        WHEN a.has_critical_security_reviews = 1
          THEN 'This GitLab instance has over 200 security report views, indicating very active security review processes. '
        WHEN a.has_high_security_reviews = 1
          THEN 'This GitLab instance allows for 50-199 security report views, indicating regular but less intensive security review activity. '
        ELSE ''
      END

      -- Add project scale assessment
      || CASE
        WHEN c.is_critical_project_scale = 1
          THEN 'This GitLab instance manages over 100 active projects, indicating large-scale development operations. '
        WHEN c.is_high_project_scale = 1
          THEN 'This GitLab instance manages between 50-99 active projects, showing significant development scale. '
        ELSE ''
      END

      -- Include specialized usage patterns
      || CASE
        WHEN a.has_diverse_security_tools = 1
          THEN 'This GitLab instance uses 3 or more different security scanners extensively (>100 jobs each). This shows the adoption of a comprehensive security approach. '
        ELSE ''
      END

      || CASE
        WHEN a.has_high_compliance_needs = 1
          THEN 'This GitLab instance heavily utilizes 3 or more compliance features, including having more than 50 merge request approval rules, more than 10 protected branches, and SonarQube integration. '
        ELSE ''
      END

      || CASE
        WHEN b.is_complex_project_integration = 1
          THEN 'This GitLab instance uses over 50 CI triggers and 10 webhooks, indicating sophisticated multi-project workflows and external integrations. '
        ELSE ''
      END

      || CASE
        WHEN b.is_critical_deployment_complexity = 1
          THEN 'This GitLab instance manages more than 10 different environments, suggesting complex deployment strategies across multiple stages or applications. '
        ELSE ''
      END

      || CASE
        WHEN b.is_critical_complexity = 1
          THEN 'This GitLab instance scores high (8+ points) across all complexity metrics, indicating sophisticated CI/CD usage across volume, integration, runners, and deployments. '
        ELSE ''
      END

      || CASE
        WHEN c.is_critical_review_complexity = 1
          THEN 'This GitLab instance manages over 50 open merge requests with 100+ unique reviewers, suggesting a sophisticated code review process. '
        ELSE ''
      END

      || CASE
        WHEN c.is_complex_collaboration = 1
          THEN 'This GitLab instance uses 100+ merge request approval rules with 50+ unique reviewers, indicating formal review processes and structured collaboration. '
        ELSE ''
      END

      || CASE
        WHEN d.is_reliability_risk = 1
          THEN 'This GitLab instance has a high deployment volume (100+ deployments) but concerning failure rates (>10%), indicating potential stability issues. '
        ELSE ''
      END

      || CASE
        WHEN d.is_complex_infrastructure = 1
          THEN 'This GitLab instance manages more than 5 environments across multiple clusters, suggesting a sophisticated deployment infrastructure. '
        ELSE ''
      END

      || CASE
        WHEN d.is_ha_ready = 1
          THEN 'This GitLab instance already uses advanced infrastructure tools (Prometheus and runners) with significant deployer activity (20+ active deployers), showing maturity in deployment practices. '
        ELSE ''
      END

      || CASE
        WHEN d.is_business_critical = 1
          THEN 'This GitLab instance combines Terraform state management, Kubernetes agents, and high deployment volumes (100+), indicating mission-critical infrastructure. '
        ELSE ''
      END

      || CASE
        WHEN d.is_critical_risk = 1
          THEN 'This GitLab instance has high-risk characteristics, combining frequent deployments (100+), complex environments (5+), and elevated failure rates (>10%), suggesting an urgent need for improved stability. '
        ELSE ''
      END

      || CASE
        WHEN d.is_critical_ha_needs = 1
          THEN 'This GitLab instance scores very high (10+ points) across deployment frequency, environment complexity, infrastructure integration, usage intensity, and business-critical features. '
        ELSE ''
      END

      || CASE
        WHEN e.is_high_integration_volume = 1
          THEN 'This GitLab instance has more than 10 webhooks in use, indicating complex system interconnections and maintenance overhead. '
        ELSE ''
      END

      || CASE
        WHEN e.is_high_cicd_fragmentation = 1
          THEN 'This GitLab instance supports using 3 or more CI/CD tools simultaneously (e.g., Jenkins, TeamCity, Bamboo), suggesting it can handle parallel pipelines and tool redundancy. '
        ELSE ''
      END

      || CASE
        WHEN e.is_high_package_diversity = 1
          THEN 'This GitLab instance uses 3+ different package registries (like NPM, Maven, PyPI), indicating a diverse technology stack and multiple storage solutions. '
        ELSE ''
      END

      || CASE
        WHEN e.is_high_messaging_overlap = 1
          THEN 'This GitLab instance has 3 or more messaging integrations (Slack, Teams, Discord, etc.), indicating fragmentation in communication tools. '
        ELSE ''
      END

      || CASE
        WHEN e.is_high_pm_overlap = 1
          THEN 'This GitLab instance allows integrating with 2 or more project management tools (Jira, YouTrack, Asana), suggesting scattered project tracking across different platforms. '
        ELSE ''
      END

      || CASE
        WHEN e.is_critical_sprawl = 1
          THEN 'This GitLab instance scores very high (10+ points) across webhook usage, CI/CD tools, package registries, messaging, and project management tools, indicating severe tool sprawl. '
        ELSE ''
      END,

      -- Default case if no significant patterns detected
      'No significant usage patterns detected.'
    )                        AS "Notable Product Usage Detail"

  FROM security_output_for_use_summary AS a
  LEFT JOIN pipeline_output_for_use_summary AS b
    ON a.dim_installation_id = b.dim_installation_id
  LEFT JOIN collaboration_output_for_use_summary AS c
    ON a.dim_installation_id = c.dim_installation_id
  LEFT JOIN availability_output_for_use_summary AS d
    ON a.dim_installation_id = d.dim_installation_id
  LEFT JOIN integration_output_for_use_summary AS e
    ON a.dim_installation_id = e.dim_installation_id

)

-- CHAMPIONSHIP WINNING FINAL SELECT STATEMENT
-- Purpose: The victory lap! Brings together all our analysis into a beautiful final output
-- Combines raw metrics, calculated scores, maturity levels, and detailed usage patterns
-- Provides the complete picture of each GitLab instance's usage and complexity
-- This is what we've been working towards - the complete instance analysis! 
SELECT
  -- The Championship Analysis - Everything We've Built! 
  product_usage_summary_aggregation.*,

  -- Instance Information
  host_name,
  gitlab_edition,
  gitlab_version,
  first_ping,
  latest_ping,
  monthly_active_users,

  -- Core Development Metrics
  push_count,
  mr_count,
  ci_build_count,
  pipeline_config_count,
  pipeline_trigger_count,
  monthly_active_developers,
  monthly_active_mr_creators,
  monthly_active_web_ide_users,

  -- DevOps Lifecycle
  deployment_count,
  successful_deployment_count,
  failed_deployment_count,
  k8s_request_count,
  release_count,
  environment_count,
  monthly_active_deployers,
  monthly_active_environment_users,

  -- Security & Compliance Champions
  sast_scan_count,
  dast_scan_count,
  container_scan_count,
  dependency_scan_count,
  secret_scan_count,
  license_scan_count,
  code_quality_scan_count,
  monthly_active_sast_users,
  monthly_active_dast_users,
  monthly_active_dependency_scan_users,
  monthly_active_container_scan_users,
  monthly_active_secret_scan_users,

  -- Collaboration & Planning Metrics
  issue_count,
  comment_count,
  project_count,
  board_count,
  epic_count,
  milestone_count,
  monthly_active_issue_creators,
  monthly_active_reviewers,
  monthly_code_review_users,

  -- Championship Scores 🏆
  core_score,
  devops_score,
  collab_score,
  security_score,

  -- The Ultimate Victory - Final Scoring and Maturity
  total_score,
  CASE
    WHEN total_score <= p20 THEN 'Minimal'     -- Just getting started
    WHEN total_score <= p55 THEN 'Light'       -- Building momentum
    WHEN total_score <= p80 THEN 'Moderate'    -- Strong competitor
    WHEN total_score <= p95 THEN 'Active'      -- Championship contender
    ELSE 'Power'                               -- DYNASTY STATUS! 
  END AS maturity_level,

  -- Detailed Scoring Breakdown - The Championship Highlights
  CONCAT(
    'Core Development Metrics (Past 3 Months):', CHR(10),
    'Total Merge Requests: ', mr_count, CHR(10),
    'Total CI Builds: ', ci_build_count, CHR(10),
    'Total Pipeline Configurations: ', pipeline_config_count, CHR(10),
    'Monthly Active Web IDE Users: ', monthly_active_web_ide_users, CHR(10),
    'Monthly Active Developers: ', monthly_active_developers, CHR(10),
    CHR(10),
    'DevOps Lifecycle Metrics (Past 3 Months):', CHR(10),
    'Successful Deployments: ', successful_deployment_count, CHR(10),
    'Total Environments: ', environment_count, CHR(10),
    'Total Releases: ', release_count, CHR(10),
    'Kubernetes Integration Usage: ', CASE WHEN k8s_request_count > 0 THEN 'Yes' ELSE 'No' END, CHR(10),
    'Monthly Active Deployers: ', monthly_active_deployers, CHR(10),
    CHR(10),
    'Collaboration Metrics (Past 3 Months):', CHR(10),
    'Monthly Active Reviewers: ', monthly_active_reviewers, CHR(10),
    'Total Comments: ', comment_count, CHR(10),
    'Total Issues Created: ', issue_count, CHR(10),
    'Total Project Boards: ', board_count, CHR(10),
    'Total Projects: ', project_count, CHR(10),
    CHR(10),
    'Security & Compliance Metrics (Past 3 Months):', CHR(10),
    'SAST Scans: ', sast_scan_count, CHR(10),
    'DAST Scans: ', dast_scan_count, CHR(10),
    'Container Scans: ', container_scan_count, CHR(10),
    'Dependency Scans: ', dependency_scan_count, CHR(10),
    'Secret Detection Scans: ', secret_scan_count, CHR(10),
    'License Scans: ', license_scan_count, CHR(10),
    'Code Quality Scans: ', code_quality_scan_count, CHR(10),
    'Monthly Active Security Scanner Users: ',
    GREATEST(
      COALESCE(monthly_active_sast_users, 0),
      COALESCE(monthly_active_dast_users, 0),
      COALESCE(monthly_active_dependency_scan_users, 0),
      COALESCE(monthly_active_container_scan_users, 0),
      COALESCE(monthly_active_secret_scan_users, 0)
    )
  )   AS scoring_details


FROM weighted_scores
LEFT JOIN product_usage_summary_aggregation
  ON weighted_scores.dim_installation_id = product_usage_summary_aggregation.dim_installation_id
ORDER BY total_score DESC
