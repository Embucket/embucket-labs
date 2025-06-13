{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{{ simple_cte([
    ('bdg_subscription_product_rate_plan', 'bdg_subscription_product_rate_plan'),
    ('seat_link', 'fct_usage_self_managed_seat_link'),
    ('ping_instance_wave', 'mart_ping_instance_metric_health_score_self_managed'),
    ('dim_host_instance_type','dim_host_instance_type')
]) }}

, instance_type_ordering AS (

    SELECT
      *,
      CASE
        WHEN instance_type = 'Production' THEN 1
        WHEN instance_type = 'Non-Production' THEN 2
        WHEN instance_type = 'Unknown' THEN 3
        ELSE 4
      END AS ordering_field
    FROM dim_host_instance_type

)

, sm_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id
    FROM bdg_subscription_product_rate_plan
    WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')

), ping_instance_wave_sm AS (

    SELECT *
    FROM ping_instance_wave
    WHERE dim_subscription_id IS NOT NULL
      AND ping_deployment_type IN ('Self-Managed', 'Dedicated')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        dim_instance_id,
        hostname,
        ping_created_date_month
      ORDER BY ping_created_at DESC
      ) = 1

), joined AS (

    SELECT
      ping_instance_wave_sm.dim_subscription_id,
      sm_subscriptions.dim_subscription_id_original,
      sm_subscriptions.dim_billing_account_id,
      ping_instance_wave_sm.ping_created_date_month                                                      AS snapshot_month,
      {{ get_date_id('ping_instance_wave_sm.ping_created_date_month') }}                                 AS snapshot_date_id,
      seat_link.report_date                                                                              AS seat_link_report_date,
      {{ get_date_id('seat_link.report_date') }}                                                         AS seat_link_report_date_id,
      ping_instance_wave_sm.dim_ping_instance_id,
      ping_instance_wave_sm.ping_created_at,
      {{ get_date_id('ping_instance_wave_sm.ping_created_at') }}                                         AS ping_created_date_id,
      ping_instance_wave_sm.dim_instance_id,
      instance_type_ordering.instance_type,
      instance_type_ordering.included_in_health_measures_str,
      ping_instance_wave_sm.hostname,
      ping_instance_wave_sm.dim_license_id,
      ping_instance_wave_sm.license_md5,
      ping_instance_wave_sm.cleaned_version,
      ping_instance_wave_sm.dim_location_country_id,
      ping_instance_wave_sm.installation_creation_date,
      ping_instance_wave_sm.dim_installation_id,
      ping_instance_wave_sm.ping_delivery_type,
      ping_instance_wave_sm.ping_deployment_type,
      ping_instance_wave_sm.is_dedicated_metric,
      ping_instance_wave_sm.is_dedicated_hostname,
      -- Wave 1
      DIV0(
          ping_instance_wave_sm.license_billable_users,
          IFNULL(ping_instance_wave_sm.license_user_count, seat_link.license_user_count)
          )                                                                                              AS license_utilization,
      ping_instance_wave_sm.license_billable_users                                                       AS billable_user_count,
      ping_instance_wave_sm.instance_user_count                                                          AS active_user_count,
      IFNULL(ping_instance_wave_sm.historical_max_user_count, seat_link.max_historical_user_count)       AS max_historical_user_count,
      IFNULL(ping_instance_wave_sm.license_user_count, seat_link.license_user_count)                     AS license_user_count,
      -- Wave 2 & 3
      ping_instance_wave_sm.umau_28_days_user,
      ping_instance_wave_sm.action_monthly_active_users_project_repo_28_days_user,
      ping_instance_wave_sm.merge_requests_28_days_user,
      ping_instance_wave_sm.projects_with_repositories_enabled_28_days_user,
      ping_instance_wave_sm.commit_comment_all_time_event,
      ping_instance_wave_sm.source_code_pushes_all_time_event,
      ping_instance_wave_sm.ci_pipelines_28_days_user,
      ping_instance_wave_sm.ci_internal_pipelines_28_days_user,
      ping_instance_wave_sm.ci_builds_28_days_user,
      ping_instance_wave_sm.ci_builds_all_time_user,
      ping_instance_wave_sm.ci_builds_all_time_event,
      ping_instance_wave_sm.ci_runners_all_time_event,
      ping_instance_wave_sm.auto_devops_enabled_all_time_event,
      ping_instance_wave_sm.gitlab_shared_runners_enabled,
      ping_instance_wave_sm.container_registry_enabled,
      ping_instance_wave_sm.template_repositories_all_time_event,
      ping_instance_wave_sm.ci_pipeline_config_repository_28_days_user,
      ping_instance_wave_sm.user_unique_users_all_secure_scanners_28_days_user,
      ping_instance_wave_sm.user_sast_jobs_28_days_user,
      ping_instance_wave_sm.user_dast_jobs_28_days_user,
      ping_instance_wave_sm.user_dependency_scanning_jobs_28_days_user,
      ping_instance_wave_sm.user_license_management_jobs_28_days_user,
      ping_instance_wave_sm.user_secret_detection_jobs_28_days_user,
      ping_instance_wave_sm.user_container_scanning_jobs_28_days_user,
      ping_instance_wave_sm.object_store_packages_enabled,
      ping_instance_wave_sm.projects_with_packages_all_time_event,
      ping_instance_wave_sm.projects_with_packages_28_days_event,
      ping_instance_wave_sm.deployments_28_days_user,
      ping_instance_wave_sm.releases_28_days_user,
      ping_instance_wave_sm.epics_28_days_user,
      ping_instance_wave_sm.issues_28_days_user,
      -- Wave 3.1
      ping_instance_wave_sm.ci_internal_pipelines_all_time_event,
      ping_instance_wave_sm.ci_external_pipelines_all_time_event,
      ping_instance_wave_sm.merge_requests_all_time_event,
      ping_instance_wave_sm.todos_all_time_event,
      ping_instance_wave_sm.epics_all_time_event,
      ping_instance_wave_sm.issues_all_time_event,
      ping_instance_wave_sm.projects_all_time_event,
      ping_instance_wave_sm.deployments_28_days_event,
      ping_instance_wave_sm.packages_28_days_event,
      ping_instance_wave_sm.dast_jobs_all_time_event,
      ping_instance_wave_sm.license_management_jobs_all_time_event,
      ping_instance_wave_sm.secret_detection_jobs_all_time_event,
      ping_instance_wave_sm.projects_jenkins_active_all_time_event,
      ping_instance_wave_sm.projects_bamboo_active_all_time_event,
      ping_instance_wave_sm.projects_jira_active_all_time_event,
      ping_instance_wave_sm.projects_drone_ci_active_all_time_event,
      ping_instance_wave_sm.projects_github_active_all_time_event,
      ping_instance_wave_sm.projects_jira_dvcs_cloud_active_all_time_event,
      ping_instance_wave_sm.projects_with_repositories_enabled_all_time_event,
      ping_instance_wave_sm.protected_branches_all_time_event,
      ping_instance_wave_sm.remote_mirrors_all_time_event,
      ping_instance_wave_sm.projects_enforcing_code_owner_approval_28_days_user,
      ping_instance_wave_sm.project_clusters_enabled_28_days_user,
      ping_instance_wave_sm.analytics_28_days_user,
      ping_instance_wave_sm.issues_edit_28_days_user,
      ping_instance_wave_sm.user_packages_28_days_user,
      ping_instance_wave_sm.terraform_state_api_28_days_user,
      ping_instance_wave_sm.incident_management_28_days_user,
      -- Wave 3.2
      ping_instance_wave_sm.auto_devops_enabled,
      ping_instance_wave_sm.gitaly_clusters_instance,
      ping_instance_wave_sm.epics_deepest_relationship_level_instance,
      ping_instance_wave_sm.network_policy_forwards_all_time_event,
      ping_instance_wave_sm.network_policy_drops_all_time_event,
      ping_instance_wave_sm.requirements_with_test_report_all_time_event,
      ping_instance_wave_sm.requirement_test_reports_ci_all_time_event,
      ping_instance_wave_sm.projects_imported_from_github_all_time_event,
      ping_instance_wave_sm.projects_jira_dvcs_server_active_all_time_event,
      ping_instance_wave_sm.service_desk_issues_all_time_event,
      ping_instance_wave_sm.ci_pipelines_all_time_user,
      ping_instance_wave_sm.service_desk_issues_28_days_user,
      ping_instance_wave_sm.projects_jira_active_28_days_user,
      ping_instance_wave_sm.projects_jira_dvcs_server_active_28_days_user,
      ping_instance_wave_sm.merge_requests_with_required_code_owners_28_days_user,
      ping_instance_wave_sm.analytics_value_stream_28_days_event,
      ping_instance_wave_sm.code_review_user_approve_mr_28_days_user,
      ping_instance_wave_sm.epics_usage_28_days_user,
      ping_instance_wave_sm.project_management_issue_milestone_changed_28_days_user,
      ping_instance_wave_sm.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      ping_instance_wave_sm.protected_branches_28_days_user,
      ping_instance_wave_sm.ci_cd_lead_time_usage_28_days_event,
      ping_instance_wave_sm.ci_cd_deployment_frequency_usage_28_days_event,
      ping_instance_wave_sm.projects_with_repositories_enabled_all_time_user,
      ping_instance_wave_sm.environments_all_time_event,
      ping_instance_wave_sm.feature_flags_all_time_event,
      ping_instance_wave_sm.successful_deployments_28_days_event,
      ping_instance_wave_sm.failed_deployments_28_days_event,
      ping_instance_wave_sm.projects_compliance_framework_all_time_event,
      ping_instance_wave_sm.commit_ci_config_file_28_days_user,
      ping_instance_wave_sm.view_audit_all_time_user,
      --Wave 5.2
      ping_instance_wave_sm.dependency_scanning_jobs_all_time_user,
      ping_instance_wave_sm.analytics_devops_adoption_all_time_user,
      ping_instance_wave_sm.projects_imported_all_time_event,
      ping_instance_wave_sm.preferences_security_dashboard_28_days_user,
      ping_instance_wave_sm.web_ide_edit_28_days_user,
      ping_instance_wave_sm.auto_devops_pipelines_all_time_event,
      ping_instance_wave_sm.projects_prometheus_active_all_time_event,
      ping_instance_wave_sm.prometheus_enabled,
      ping_instance_wave_sm.prometheus_metrics_enabled,
      ping_instance_wave_sm.group_saml_enabled,
      ping_instance_wave_sm.jira_issue_imports_all_time_event,
      ping_instance_wave_sm.author_epic_all_time_user,
      ping_instance_wave_sm.author_issue_all_time_user,
      ping_instance_wave_sm.failed_deployments_28_days_user,
      ping_instance_wave_sm.successful_deployments_28_days_user,
      -- Wave 5.3
      ping_instance_wave_sm.geo_enabled,
      ping_instance_wave_sm.auto_devops_pipelines_28_days_user,
      ping_instance_wave_sm.active_instance_runners_all_time_event,
      ping_instance_wave_sm.active_group_runners_all_time_event,
      ping_instance_wave_sm.active_project_runners_all_time_event,
      TO_VARCHAR(ping_instance_wave_sm.gitaly_version)                                                   AS gitaly_version,
      ping_instance_wave_sm.gitaly_servers_all_time_event,
      -- Wave 6
      ping_instance_wave_sm.api_fuzzing_scans_all_time_event,
      ping_instance_wave_sm.api_fuzzing_scans_28_days_event,
      ping_instance_wave_sm.coverage_fuzzing_scans_all_time_event,
      ping_instance_wave_sm.coverage_fuzzing_scans_28_days_event,
      ping_instance_wave_sm.secret_detection_scans_all_time_event,
      ping_instance_wave_sm.secret_detection_scans_28_days_event,
      ping_instance_wave_sm.dependency_scanning_scans_all_time_event,
      ping_instance_wave_sm.dependency_scanning_scans_28_days_event,
      ping_instance_wave_sm.container_scanning_scans_all_time_event,
      ping_instance_wave_sm.container_scanning_scans_28_days_event,
      ping_instance_wave_sm.dast_scans_all_time_event,
      ping_instance_wave_sm.dast_scans_28_days_event,
      ping_instance_wave_sm.sast_scans_all_time_event,
      ping_instance_wave_sm.sast_scans_28_days_event,
      -- Wave 6.1
      ping_instance_wave_sm.packages_pushed_registry_all_time_event,
      ping_instance_wave_sm.packages_pulled_registry_all_time_event,
      ping_instance_wave_sm.compliance_dashboard_view_28_days_user,
      ping_instance_wave_sm.audit_screen_view_28_days_user,
      ping_instance_wave_sm.instance_audit_screen_view_28_days_user,
      ping_instance_wave_sm.credential_inventory_view_28_days_user,
      ping_instance_wave_sm.compliance_frameworks_pipeline_all_time_event,
      ping_instance_wave_sm.compliance_frameworks_pipeline_28_days_event,
      ping_instance_wave_sm.groups_streaming_destinations_all_time_event,
      ping_instance_wave_sm.audit_event_destinations_all_time_event,
      ping_instance_wave_sm.audit_event_destinations_28_days_event,
      ping_instance_wave_sm.projects_status_checks_all_time_event,
      ping_instance_wave_sm.external_status_checks_all_time_event,
      ping_instance_wave_sm.paid_license_search_28_days_user,
      ping_instance_wave_sm.last_activity_28_days_user,
      -- Wave 7
      ping_instance_wave_sm.snippets_28_days_event,
      ping_instance_wave_sm.single_file_editor_28_days_user,
      ping_instance_wave_sm.merge_requests_created_28_days_event,
      ping_instance_wave_sm.merge_requests_created_28_days_user,
      ping_instance_wave_sm.merge_requests_approval_rules_28_days_event,
      ping_instance_wave_sm.custom_compliance_frameworks_28_days_event,
      ping_instance_wave_sm.projects_security_policy_28_days_event,
      ping_instance_wave_sm.merge_requests_security_policy_28_days_user,
      ping_instance_wave_sm.pipelines_implicit_auto_devops_28_days_event,
      ping_instance_wave_sm.pipeline_schedules_28_days_user,
      -- Wave 8
      ping_instance_wave_sm.ci_internal_pipelines_28_days_event,
      -- Wave 9
      ping_instance_wave_sm.ci_builds_28_days_event,
      ping_instance_wave_sm.groups_all_time_event,
      ping_instance_wave_sm.commit_ci_config_file_7_days_user,
      ping_instance_wave_sm.ci_pipeline_config_repository_all_time_user,
      ping_instance_wave_sm.ci_pipeline_config_repository_all_time_event,
      ping_instance_wave_sm.pipeline_schedules_all_time_event,
      ping_instance_wave_sm.pipeline_schedules_all_time_user,
      -- CI Lighthouse Metric
      ping_instance_wave_sm.ci_builds_28_days_event_post_16_9,
      -- Data Quality Flags
      IFF(ping_instance_wave_sm.instance_user_count != seat_link.active_user_count,
          ping_instance_wave_sm.instance_user_count, NULL)                                               AS instance_user_count_not_aligned,
      IFF(ping_instance_wave_sm.historical_max_user_count != seat_link.max_historical_user_count,
          ping_instance_wave_sm.historical_max_user_count, NULL)                                         AS historical_max_users_not_aligned,
      seat_link.is_subscription_in_zuora                                                                 AS is_seat_link_subscription_in_zuora,
      seat_link.is_rate_plan_in_zuora                                                                    AS is_seat_link_rate_plan_in_zuora,
      seat_link.is_active_user_count_available                                                           AS is_seat_link_active_user_count_available,
      ping_instance_wave_sm.is_license_mapped_to_subscription                                            AS is_usage_ping_license_mapped_to_subscription,
      ping_instance_wave_sm.is_license_subscription_id_valid                                             AS is_usage_ping_license_subscription_id_valid,
      IFF(ping_instance_wave_sm.ping_created_at IS NOT NULL
            OR seat_link.report_date IS NOT NULL,
          TRUE, FALSE)                                                                                   AS is_data_in_subscription_month,
      IFF(is_data_in_subscription_month = TRUE AND
            ROW_NUMBER() OVER (PARTITION BY
                                sm_subscriptions.dim_subscription_id,
                                ping_instance_wave_sm.dim_instance_id,
                                ping_instance_wave_sm.hostname,
                                is_data_in_subscription_month
                               ORDER BY ping_instance_wave_sm.ping_created_date_month DESC
                            ) = 1,
          TRUE, FALSE)                                                                                   AS is_latest_data
    FROM ping_instance_wave_sm
    JOIN sm_subscriptions
      ON sm_subscriptions.dim_subscription_id = ping_instance_wave_sm.dim_subscription_id
    LEFT JOIN seat_link
      ON ping_instance_wave_sm.dim_subscription_id = seat_link.dim_subscription_id
      AND ping_instance_wave_sm.ping_created_date_month = seat_link.snapshot_month
    LEFT JOIN instance_type_ordering
      ON ping_instance_wave_sm.dim_instance_id = instance_type_ordering.instance_uuid
      AND ping_instance_wave_sm.hostname = instance_type_ordering.instance_hostname
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        ping_instance_wave_sm.ping_created_date_month,
        sm_subscriptions.dim_subscription_id_original,
        ping_instance_wave_sm.dim_instance_id,
        ping_instance_wave_sm.hostname
        ORDER BY
          ping_instance_wave_sm.ping_created_at DESC,
          instance_type_ordering.ordering_field ASC --prioritizing Production instances
    ) = 1

)

SELECT
  *
FROM joined
