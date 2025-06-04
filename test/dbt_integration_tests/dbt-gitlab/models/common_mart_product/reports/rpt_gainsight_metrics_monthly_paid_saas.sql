{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{{ simple_cte([
    ('saas_usage_ping', 'mart_ping_namespace_metric_health_score_saas'),
    ('bdg_subscription_product_rate_plan', 'bdg_subscription_product_rate_plan'),
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('dates', 'dim_date'),
    ('charges', 'fct_charge')
]) }}

, saas_subscriptions AS (

    SELECT DISTINCT
      bdg_subscription_product_rate_plan.dim_subscription_id,
      bdg_subscription_product_rate_plan.dim_billing_account_id
    FROM bdg_subscription_product_rate_plan
    LEFT JOIN charges
      ON charges.dim_subscription_id = bdg_subscription_product_rate_plan.dim_subscription_id
    WHERE bdg_subscription_product_rate_plan.product_deployment_type = 'GitLab.com'
      AND (
        bdg_subscription_product_rate_plan.product_rate_plan_charge_name NOT IN (
          '1,000 CI Minutes',
          'Gitlab Storage 10GB - 1 Year',
          'Premium Support',
          '1,000 Compute Minutes'
        )
        OR charges.charge_type != 'OneTime'
      )

), gitlab_seats AS (

    SELECT
      gitlab_subscriptions.namespace_id,
      gitlab_subscriptions.seats,
      gitlab_subscriptions.seats_in_use,
      gitlab_subscriptions.max_seats_used,
      dates.first_day_of_month                                                          AS snapshot_month
    FROM gitlab_subscriptions
    INNER JOIN dates
      ON dates.date_actual BETWEEN TO_DATE(gitlab_subscriptions.valid_from)
                            AND IFNULL(gitlab_subscriptions.valid_to, CURRENT_DATE)
    -- Choosing the last subscription of the month
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        gitlab_subscriptions.namespace_id,
        dates.first_day_of_month
      ORDER BY gitlab_subscriptions.valid_from DESC
    ) = 1

), joined AS (

    SELECT
      saas_usage_ping.dim_subscription_id,
      saas_usage_ping.dim_subscription_id_original,
      saas_subscriptions.dim_billing_account_id,
      saas_usage_ping.dim_namespace_id,
      saas_usage_ping.reporting_month                                                   AS snapshot_month,
      {{ get_date_id('saas_usage_ping.reporting_month') }}                              AS snapshot_date_id,
      saas_usage_ping.ping_date                                                         AS ping_created_at,
      {{ get_date_id('saas_usage_ping.ping_date') }}                                    AS ping_created_date_id,
      saas_usage_ping.instance_type,
      saas_usage_ping.included_in_health_measures_str,
      'SaaS'                                                                            AS ping_delivery_type,
      'GitLab.com'                                                                      AS ping_deployment_type,
      -- Wave 1
      gitlab_seats.seats                                                                AS subscription_seats,
      gitlab_seats.seats_in_use                                                         AS billable_user_count,
      DIV0(gitlab_seats.seats_in_use, gitlab_seats.seats)                               AS license_utilization,
      gitlab_seats.max_seats_used                                                       AS max_historical_user_count,
      -- Wave 2 & 3
      "usage_activity_by_stage_monthly.manage.events"                                           AS umau_28_days_user,
      "usage_activity_by_stage_monthly.create.action_monthly_active_users_project_repo"         AS action_monthly_active_users_project_repo_28_days_user,
      "usage_activity_by_stage_monthly.create.merge_requests"                                   AS merge_requests_28_days_user,
      "usage_activity_by_stage_monthly.create.projects_with_repositories_enabled"               AS projects_with_repositories_enabled_28_days_user,
      "counts.commit_comment"                                                                   AS commit_comment_all_time_event,
      "counts.source_code_pushes"                                                               AS source_code_pushes_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipelines"                                     AS ci_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_internal_pipelines"                            AS ci_internal_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_builds"                                        AS ci_builds_28_days_user,
      "usage_activity_by_stage.verify.ci_builds"                                                AS ci_builds_all_time_user,
      "counts.ci_builds"                                                                        AS ci_builds_all_time_event,
      "counts.ci_runners"                                                                       AS ci_runners_all_time_event,
      "counts.auto_devops_enabled"                                                              AS auto_devops_enabled_all_time_event,
      "gitlab_shared_runners_enabled"                                                           AS gitlab_shared_runners_enabled,
      "container_registry_enabled"                                                              AS container_registry_enabled,
      "counts.template_repositories"                                                            AS template_repositories_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipeline_config_repository"                    AS ci_pipeline_config_repository_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_unique_users_all_secure_scanners"            AS user_unique_users_all_secure_scanners_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_sast_jobs"                                   AS user_sast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dast_jobs"                                   AS user_dast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dependency_scanning_jobs"                    AS user_dependency_scanning_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_license_management_jobs"                     AS user_license_management_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_secret_detection_jobs"                       AS user_secret_detection_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_container_scanning_jobs"                     AS user_container_scanning_jobs_28_days_user,
      "object_store.packages.enabled"                                                           AS object_store_packages_enabled,
      "counts.projects_with_packages"                                                           AS projects_with_packages_all_time_event,
      "usage_activity_by_stage_monthly.package.projects_with_packages"                          AS projects_with_packages_28_days_event,
      "usage_activity_by_stage_monthly.release.deployments"                                     AS deployments_28_days_user,
      "usage_activity_by_stage_monthly.release.releases"                                        AS releases_28_days_user,
      "usage_activity_by_stage_monthly.plan.epics"                                              AS epics_28_days_user,
      "usage_activity_by_stage_monthly.plan.issues"                                             AS issues_28_days_user,
      -- Wave 3.1
      "counts.ci_internal_pipelines"                                                            AS ci_internal_pipelines_all_time_event,
      "counts.ci_external_pipelines"                                                            AS ci_external_pipelines_all_time_event,
      "counts.merge_requests"                                                                   AS merge_requests_all_time_event,
      "counts.todos"                                                                            AS todos_all_time_event,
      "counts.epics"                                                                            AS epics_all_time_event,
      "counts.issues"                                                                           AS issues_all_time_event,
      "counts.projects"                                                                         AS projects_all_time_event,
      "counts_monthly.deployments"                                                              AS deployments_28_days_event,
      "counts_monthly.packages"                                                                 AS packages_28_days_event,
      "counts.dast_jobs"                                                                        AS dast_jobs_all_time_event,
      "counts.license_management_jobs"                                                          AS license_management_jobs_all_time_event,
      "counts.secret_detection_jobs"                                                            AS secret_detection_jobs_all_time_event,
      "counts.projects_jenkins_active"                                                          AS projects_jenkins_active_all_time_event,
      "counts.projects_bamboo_active"                                                           AS projects_bamboo_active_all_time_event,
      "counts.projects_jira_active"                                                             AS projects_jira_active_all_time_event,
      "counts.projects_drone_ci_active"                                                         AS projects_drone_ci_active_all_time_event,
      "counts.projects_github_active"                                                           AS projects_github_active_all_time_event,
      "counts.projects_jira_dvcs_cloud_active"                                                  AS projects_jira_dvcs_cloud_active_all_time_event,
      "counts.projects_with_repositories_enabled"                                               AS projects_with_repositories_enabled_all_time_event,
      "counts.protected_branches"                                                               AS protected_branches_all_time_event,
      "counts.remote_mirrors"                                                                   AS remote_mirrors_all_time_event,
      "usage_activity_by_stage.create.projects_enforcing_code_owner_approval"                   AS projects_enforcing_code_owner_approval_28_days_user,
      "usage_activity_by_stage_monthly.configure.project_clusters_enabled"                      AS project_clusters_enabled_28_days_user,
      "redis_hll_counters.analytics.analytics_total_unique_counts_monthly"                      AS analytics_28_days_user,
      "redis_hll_counters.issues_edit.issues_edit_total_unique_counts_monthly"                  AS issues_edit_28_days_user,
      "redis_hll_counters.user_packages.user_packages_total_unique_counts_monthly"              AS user_packages_28_days_user,
      "redis_hll_counters.terraform.p_terraform_state_api_unique_users_monthly"                 AS terraform_state_api_28_days_user,
      "redis_hll_counters.incident_management.incident_management_total_unique_counts_monthly"  AS incident_management_28_days_user,
      -- Wave 3.2
      "instance_auto_devops_enabled"                                                            AS auto_devops_enabled,
      "gitaly.clusters"                                                                         AS gitaly_clusters_instance,
      "counts.epics_deepest_relationship_level"                                                 AS epics_deepest_relationship_level_instance,
      "counts.network_policy_forwards"                                                          AS network_policy_forwards_all_time_event,
      "counts.network_policy_drops"                                                             AS network_policy_drops_all_time_event,
      "counts.requirements_with_test_report"                                                    AS requirements_with_test_report_all_time_event,
      "counts.requirement_test_reports_ci"                                                      AS requirement_test_reports_ci_all_time_event,
      "counts.projects_imported_from_github"                                                    AS projects_imported_from_github_all_time_event,
      "counts.projects_jira_dvcs_server_active"                                                 AS projects_jira_dvcs_server_active_all_time_event,
      "counts.service_desk_issues"                                                              AS service_desk_issues_all_time_event,
      "usage_activity_by_stage.verify.ci_pipelines"                                             AS ci_pipelines_all_time_user,
      "usage_activity_by_stage_monthly.plan.service_desk_issues"                                AS service_desk_issues_28_days_user,
      "usage_activity_by_stage_monthly.plan.projects_jira_active"                               AS projects_jira_active_28_days_user,
      "usage_activity_by_stage_monthly.plan.projects_jira_dvcs_server_active"                   AS projects_jira_dvcs_server_active_28_days_user,
      "usage_activity_by_stage_monthly.create.merge_requests_with_required_codeowners"          AS merge_requests_with_required_code_owners_28_days_user,
      "redis_hll_counters.analytics.g_analytics_valuestream_monthly"                            AS analytics_value_stream_28_days_event,
      "redis_hll_counters.code_review.i_code_review_user_approve_mr_monthly"                    AS code_review_user_approve_mr_28_days_user,
      "redis_hll_counters.epics_usage.epics_usage_total_unique_counts_monthly"                  AS epics_usage_28_days_user,
      "redis_hll_counters.issues_edit.g_project_management_issue_milestone_changed_monthly"     AS project_management_issue_milestone_changed_28_days_user,
      "redis_hll_counters.issues_edit.g_project_management_issue_iteration_changed_monthly"     AS project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      "usage_activity_by_stage_monthly.create.protected_branches"                               AS protected_branches_28_days_user,
      "redis_hll_counters.analytics.p_analytics_ci_cd_lead_time_monthly"                        AS ci_cd_lead_time_usage_28_days_event,
      "redis_hll_counters.analytics.p_analytics_ci_cd_deployment_frequency_monthly"             AS ci_cd_deployment_frequency_usage_28_days_event,
      "usage_activity_by_stage.create.projects_with_repositories_enabled"                       AS projects_with_repositories_enabled_all_time_user,
      "counts.environments"                                                                     AS environments_all_time_event,
      "counts.feature_flags"                                                                    AS feature_flags_all_time_event,
      "counts_monthly.successful_deployments"                                                   AS successful_deployments_28_days_event,
      "counts_monthly.failed_deployments"                                                       AS failed_deployments_28_days_event,
      "usage_activity_by_stage_monthly.manage.projects_with_compliance_framework"               AS projects_compliance_framework_all_time_event,
      "redis_hll_counters.pipeline_authoring.o_pipeline_authoring_unique_users_committing_ciconfigfile_monthly" AS commit_ci_config_file_28_days_user,
      "compliance_unique_visits.g_compliance_audit_events"                                      AS view_audit_all_time_user,
      -- Wave 5.2
      "usage_activity_by_stage.secure.user_dependency_scanning_jobs"                            AS dependency_scanning_jobs_all_time_user,
      "analytics_unique_visits.i_analytics_dev_ops_adoption"                                    AS analytics_devops_adoption_all_time_user,
      "usage_activity_by_stage.manage.project_imports.total"                                    AS projects_imported_all_time_event,
      "usage_activity_by_stage_monthly.secure.user_preferences_group_overview_security_dashboard"  AS preferences_security_dashboard_28_days_user,
      "usage_activity_by_stage_monthly.create.action_monthly_active_users_ide_edit"             AS web_ide_edit_28_days_user,
      "counts.ci_pipeline_config_auto_devops"                                                   AS auto_devops_pipelines_all_time_event,
      "counts.projects_prometheus_active"                                                       AS projects_prometheus_active_all_time_event,
      "prometheus_enabled"                                                                      AS prometheus_enabled,
      "prometheus_metrics_enabled"                                                              AS prometheus_metrics_enabled,
      "usage_activity_by_stage.manage.group_saml_enabled"                                       AS group_saml_enabled,
      "usage_activity_by_stage.manage.issue_imports.jira"                                       AS jira_issue_imports_all_time_event,
      "usage_activity_by_stage.plan.epics"                                                      AS author_epic_all_time_user,
      "usage_activity_by_stage.plan.issues"                                                     AS author_issue_all_time_user,
      "usage_activity_by_stage_monthly.release.failed_deployments"                              AS failed_deployments_28_days_user,
      "usage_activity_by_stage_monthly.release.successful_deployments"                          AS successful_deployments_28_days_user,
      -- Wave 5.3
      "geo_enabled"                                                                             AS geo_enabled,
      "usage_activity_by_stage_monthly.verify.ci_pipeline_config_auto_devops"                   AS auto_devops_pipelines_28_days_user,
      "counts.ci_runners_instance_type_active"                                                  AS active_instance_runners_all_time_event,
      "counts.ci_runners_group_type_active"                                                     AS active_group_runners_all_time_event,
      "counts.ci_runners_project_type_active"                                                   AS active_project_runners_all_time_event,
      "gitaly.version"::VARCHAR                                                                 AS gitaly_version,
      "gitaly.servers"                                                                          AS gitaly_servers_all_time_event,
      -- Wave 6.0
      "usage_activity_by_stage.secure.api_fuzzing_scans"                                        AS api_fuzzing_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.api_fuzzing_scans"                                AS api_fuzzing_scans_28_days_event,
      "usage_activity_by_stage.secure.coverage_fuzzing_scans"                                   AS coverage_fuzzing_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.coverage_fuzzing_scans"                           AS coverage_fuzzing_scans_28_days_event,
      "usage_activity_by_stage.secure.secret_detection_scans"                                   AS secret_detection_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.secret_detection_scans"                           AS secret_detection_scans_28_days_event,
      "usage_activity_by_stage.secure.dependency_scanning_scans"                                AS dependency_scanning_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.dependency_scanning_scans"                        AS dependency_scanning_scans_28_days_event,
      "usage_activity_by_stage.secure.container_scanning_scans"                                 AS container_scanning_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.container_scanning_scans"                         AS container_scanning_scans_28_days_event,
      "usage_activity_by_stage.secure.dast_scans"                                               AS dast_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.dast_scans"                                       AS dast_scans_28_days_event,
      "usage_activity_by_stage.secure.sast_scans"                                               AS sast_scans_all_time_event,
      "usage_activity_by_stage_monthly.secure.sast_scans"                                       AS sast_scans_28_days_event,
      -- Wave 6.1
      "counts.package_events_i_package_push_package_by_deploy_token"                            AS packages_pushed_registry_all_time_event,
      "counts.package_events_i_package_pull_package_by_guest"                                   AS packages_pulled_registry_all_time_event,
      "redis_hll_counters.compliance.g_compliance_dashboard_monthly"                            AS compliance_dashboard_view_28_days_user,
      "redis_hll_counters.compliance.g_compliance_audit_events_monthly"                         AS audit_screen_view_28_days_user,
      "redis_hll_counters.compliance.i_compliance_audit_events_monthly"                         AS instance_audit_screen_view_28_days_user,
      "redis_hll_counters.compliance.i_compliance_credential_inventory_monthly"                 AS credential_inventory_view_28_days_user,
      "usage_activity_by_stage.manage.compliance_frameworks_with_pipeline"                      AS compliance_frameworks_pipeline_all_time_event,
      "usage_activity_by_stage_monthly.manage.compliance_frameworks_with_pipeline"              AS compliance_frameworks_pipeline_28_days_event,
      "usage_activity_by_stage.manage.groups_with_event_streaming_destinations"                 AS groups_streaming_destinations_all_time_event,
      "usage_activity_by_stage.manage.audit_event_destinations"                                 AS audit_event_destinations_all_time_event,
      "usage_activity_by_stage_monthly.manage.audit_event_destinations"                         AS audit_event_destinations_28_days_event,
      "counts.projects_with_external_status_checks"                                             AS projects_status_checks_all_time_event,
      "counts.external_status_checks"                                                           AS external_status_checks_all_time_event,
      "redis_hll_counters.search.i_search_paid_monthly"                                         AS paid_license_search_28_days_user,
      "redis_hll_counters.manage.unique_active_users_monthly"                                   AS last_activity_28_days_user,
      -- Wave 7
      "counts_monthly.snippets"                                                                                             AS snippets_28_days_event,
      "redis_hll_counters.ide_edit.g_edit_by_sfe_monthly"                                                                   AS single_file_editor_28_days_user,
      "redis_hll_counters.code_review.i_code_review_create_mr_monthly"                                                      AS merge_requests_created_28_days_event,
      "redis_hll_counters.code_review.i_code_review_user_create_mr_monthly"                                                 AS merge_requests_created_28_days_user,
      "usage_activity_by_stage_monthly.govern.merged_merge_requests_using_approval_rules_distinct"                          AS merge_requests_approval_rules_28_days_event,
      "usage_activity_by_stage_monthly.manage.custom_compliance_frameworks"                                                 AS custom_compliance_frameworks_28_days_event,
      "usage_activity_by_stage_monthly.govern.distinct_count_project_id_from_security_orchestration_policy_configurations"  AS projects_security_policy_28_days_event,
      "usage_activity_by_stage_monthly.govern.user_merge_requests_for_projects_with_assigned_security_policy_project"       AS merge_requests_security_policy_28_days_user,
      "redis_hll_counters.ci_templates.p_ci_templates_implicit_auto_devops_monthly"                                         AS pipelines_implicit_auto_devops_28_days_event,
      "usage_activity_by_stage_monthly.verify.ci_pipeline_schedules"                                                        AS pipeline_schedules_28_days_user,
      -- Wave 8
      "counts_monthly.ci_internal_pipelines"    AS ci_internal_pipelines_28_days_event,
      -- Wave 9
      "counts_monthly.ci_builds"                                                                                AS ci_builds_28_days_event,
      "counts.groups"                                                                                           AS groups_all_time_event,
      "redis_hll_counters.pipeline_authoring.o_pipeline_authoring_unique_users_committing_ciconfigfile_weekly"  AS commit_ci_config_file_7_days_user,
      "usage_activity_by_stage.verify.ci_pipeline_config_repository"                                            AS ci_pipeline_config_repository_all_time_user,
      "counts.ci_pipeline_config_repository"                                                                    AS ci_pipeline_config_repository_all_time_event,
      "counts.ci_pipeline_schedules"                                                                            AS pipeline_schedules_all_time_event,
      "usage_activity_by_stage.verify.ci_pipeline_schedules"                                                    AS pipeline_schedules_all_time_user,
      -- CI Lighthouse Metric
      "counts.count_total_create_ci_build_monthly"  AS ci_builds_28_days_event_post_16_9,
      -- Data Quality Flags
      IFF(license_utilization = 0
            AND billable_user_count > 0,
          TRUE, FALSE)                                                                  AS is_missing_paid_seats,
      IFF(saas_usage_ping.reporting_month IS NOT NULL
            OR gitlab_seats.snapshot_month IS NOT NULL,
          TRUE, FALSE)                                                                  AS is_data_in_subscription_month,
      IFF(is_data_in_subscription_month = TRUE AND
            ROW_NUMBER() OVER (PARTITION BY
                                saas_usage_ping.dim_subscription_id,
                                saas_usage_ping.dim_namespace_id,
                                is_data_in_subscription_month
                               ORDER BY saas_usage_ping.reporting_month DESC
                            ) = 1,
          TRUE, FALSE)                                                                        AS is_latest_data
    FROM saas_usage_ping
    JOIN saas_subscriptions
      ON saas_subscriptions.dim_subscription_id = saas_usage_ping.dim_subscription_id
    LEFT JOIN gitlab_seats
      ON saas_usage_ping.dim_namespace_id = gitlab_seats.namespace_id
      AND saas_usage_ping.reporting_month = gitlab_seats.snapshot_month

)

SELECT
  *
FROM joined
