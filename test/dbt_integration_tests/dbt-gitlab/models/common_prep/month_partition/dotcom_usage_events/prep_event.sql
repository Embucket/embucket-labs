{{ config(
    tags=["product"]
) }}

{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(2)).strftime('%Y')) %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(2)).strftime('%m')) %}


{%- set event_ctes = [
  {
    "event_name": "action",
    "source_cte_name": "prep_action",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "dast_build_run",
    "source_cte_name": "dast_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "dependency_scanning_build_run",
    "source_cte_name": "dependency_scanning_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "deployment_creation",
    "source_cte_name": "prep_deployment",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_deployment_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "epic_creation",
    "source_cte_name": "prep_epic",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "NULL",
    "primary_key": "epic_id",
    "plan_column_name": "dim_plan_id_at_creation",
    "created_date_name": "dim_created_date_id"
  },
  {
    "event_name": "issue_creation_other",
    "source_cte_name": "issue_creation_other_source",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "project_id",
    "primary_key": "issue_id",
    "plan_column_name": "dim_plan_id_at_creation",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "issue_note_creation",
    "source_cte_name": "issue_note",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "license_scanning_build_run",
    "source_cte_name": "license_scanning_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "merge_request_creation",
    "source_cte_name": "prep_merge_request",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "project_id",
    "primary_key": "merge_request_id",
    "plan_column_name": "dim_plan_id_at_creation",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "merge_request_note_creation",
    "source_cte_name": "merge_request_note",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "ci_pipeline_creation",
    "source_cte_name": "prep_ci_pipeline",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_pipeline_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "package_creation",
    "source_cte_name": "prep_package",
    "user_column_name": "creator_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "package_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "container_scanning_build_run",
    "source_cte_name": "protect_ci_build",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "push_action",
    "source_cte_name": "push_actions",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "release_creation",
    "source_cte_name": "prep_release",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_release_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "requirement_creation",
    "source_cte_name": "prep_requirement",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "requirement_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "sast_build_run",
    "source_cte_name": "sast_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "secret_detection_build_run",
    "source_cte_name": "secret_detection_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "other_ci_build_creation",
    "source_cte_name": "other_ci_build",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "successful_ci_pipeline_creation",
    "source_cte_name": "successful_ci_pipelines",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_pipeline_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "action_monthly_active_users_project_repo",
    "source_cte_name": "monthly_active_users_project_repo",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "ci_stages",
    "source_cte_name": "prep_ci_stage",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_stage_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "notes_other",
    "source_cte_name": "other_notes_source",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "todos",
    "source_cte_name": "prep_todo",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_todo_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "issue_resource_label_events",
    "source_cte_name": "issue_resource_label_events",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_issue_label_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "environments",
    "source_cte_name": "prep_environment_event",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_environment_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "issue_resource_milestone_events",
    "source_cte_name": "issue_resource_milestone",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_resource_milestone_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "labels",
    "source_cte_name": "prep_labels",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_label_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "terraform_reports",
    "source_cte_name": "terraform_reports_events",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_job_artifact_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "users_created",
    "source_cte_name": "prep_user_event",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "NULL",
    "project_column_name": "NULL",
    "primary_key": "dim_user_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "action_monthly_active_users_wiki_repo",
    "source_cte_name": "action_monthly_active_users_wiki_repo_source",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "epic_notes",
    "source_cte_name": "epic_notes_source",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "NULL",
    "primary_key": "dim_note_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "boards",
    "source_cte_name": "prep_board",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_board_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "project_auto_devops",
    "source_cte_name": "prep_project_auto_devops",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_auto_devops_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "integrations",
    "source_cte_name": "prep_integration",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "integration_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "issue_resource_weight_events",
    "source_cte_name": "prep_issue_resource_weight",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_resource_weight_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "milestones",
    "source_cte_name": "prep_milestone",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_milestone_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "action_monthly_active_users_design_management",
    "source_cte_name": "action_monthly_active_users_design_management_source",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "ci_pipeline_schedules",
    "source_cte_name": "prep_ci_pipeline_schedule",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_pipeline_schedule_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "snippets",
    "source_cte_name": "prep_snippet",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "snippet_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "projects_prometheus_active",
    "source_cte_name": "project_prometheus_source",
    "user_column_name": "dim_user_id_creator",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_project_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "ci_triggers",
    "source_cte_name": "prep_ci_trigger",
    "user_column_name": "owner_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_trigger_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "incident_labeled_issues",
    "source_cte_name": "incident_labeled_issues_source",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "project_id",
    "primary_key": "issue_id",
    "plan_column_name": "dim_plan_id_at_creation",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "api_fuzzing_build_run",
    "source_cte_name": "api_fuzzing_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "kubernetes_with_agent",
    "source_cte_name": "prep_cluster_agent",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "cluster_agent_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  },
  {
    "event_name": "accept_invite",
    "source_cte_name": "prep_member_accepted_invites",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "NULL",
    "primary_key": "dim_member_id",
    "plan_column_name": "dim_plan_id",
    "created_date_name": "created_date_id"
  }
]

-%}

{{ simple_cte([
    ('prep_ci_pipeline', 'prep_ci_pipeline'),
    ('prep_action', 'prep_action'),
    ('prep_ci_build', 'prep_ci_build'),
    ('prep_deployment', 'prep_deployment'),
    ('prep_epic', 'prep_epic'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request'),
    ('prep_note', 'prep_note'),
    ('prep_package', 'prep_package'),
    ('prep_release', 'prep_release'),
    ('prep_requirement', 'prep_requirement'),
    ('dim_project', 'dim_project'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_user', 'prep_user'),
    ('prep_plan', 'prep_gitlab_dotcom_plan'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('prep_ci_stage', 'prep_ci_stage'),
    ('prep_todo', 'prep_todo'),
    ('prep_resource_label', 'prep_resource_label'),
    ('prep_environment_event', 'prep_environment_event'),
    ('prep_resource_milestone', 'prep_resource_milestone'),
    ('prep_labels', 'prep_labels'),
    ('prep_ci_artifact', 'prep_ci_artifact'),
    ('prep_user_event', 'prep_user'),
    ('prep_board', 'prep_board'),
    ('prep_project_auto_devops', 'prep_project_auto_devops'),
    ('prep_integration', 'prep_integration'),
    ('prep_issue_resource_weight', 'prep_issue_resource_weight'),
    ('prep_milestone', 'prep_milestone'),
    ('prep_ci_pipeline_schedule', 'prep_ci_pipeline_schedule'),
    ('prep_snippet', 'prep_snippet'),
    ('prep_project', 'prep_project'),
    ('prep_ci_trigger', 'prep_ci_trigger'),
    ('prep_cluster_agent', 'prep_cluster_agent'),
    ('prep_member_accepted_invites', 'prep_member_accepted_invites')
]) }}

, dast_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'dast'

), dependency_scanning_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'dependency_scanning'

), push_actions AS (

    SELECT *
    FROM  prep_action
    WHERE event_action_type = 'pushed'

), issue_note AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type = 'Issue'

), license_scanning_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type IN (
                                  'license_scanning',
                                  'license_management'
                                )

), merge_request_note AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type = 'MergeRequest'

), protect_ci_build AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'container_scanning'

), sast_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'sast'

), secret_detection_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'secret_detection'

), other_ci_build AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type IS NULL

), api_fuzzing_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'api_fuzzing'

), successful_ci_pipelines AS (

    SELECT *
    FROM prep_ci_pipeline
    WHERE failure_reason IS NULL

), monthly_active_users_project_repo AS (

    SELECT *
    FROM  prep_action
    WHERE target_type IS NULL
      AND event_action_type = 'pushed'

), issue_resource_label_events AS (

    SELECT *
    FROM prep_resource_label
    WHERE dim_issue_sk IS NOT NULL

), issue_resource_milestone AS (

    SELECT *
    FROM prep_resource_milestone
    WHERE dim_issue_sk IS NOT NULL

), terraform_reports_events AS (

    SELECT *
    FROM prep_ci_artifact
    WHERE file_type = 18

), action_monthly_active_users_wiki_repo_source AS (

    SELECT *
    FROM  prep_action
    WHERE target_type = 'WikiPage::Meta'
      AND event_action_type IN ('created', 'updated')

), other_notes_source AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type NOT IN ('Epic', 'MergeRequest')

), epic_notes_source AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type = 'Epic'

), action_monthly_active_users_design_management_source AS (

    SELECT *
    FROM  prep_action
    WHERE target_type = 'DesignManagement::Design'
      AND event_action_type IN ('created', 'updated')

), project_prometheus_source AS (

    SELECT *,
      dim_date_id AS created_date_id
    FROM  prep_project
    WHERE ARRAY_CONTAINS('Integrations::Prometheus'::VARIANT, active_service_types_array)

), incident_labeled_issues_source AS (

    SELECT
      *
    FROM prep_issue
    WHERE ARRAY_CONTAINS('incident'::variant, labels)

), issue_creation_other_source AS (

    SELECT
      *
    FROM prep_issue
    WHERE NOT ARRAY_CONTAINS('incident'::variant, labels)

), data AS (

{% for event_cte in event_ctes %}

    SELECT
      MD5({{ event_cte.source_cte_name}}.{{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}')   AS event_pk,
      {{ event_cte.source_cte_name}}.{{ event_cte.primary_key }}                                               AS event_id,
      '{{ event_cte.event_name }}'                                                                             AS event_name,
      {{ event_cte.source_cte_name}}.created_at                                                                AS event_created_at,
      {{ event_cte.source_cte_name}}.{{ event_cte.created_date_name }}                                         AS created_date_id,
      {%- if event_cte.project_column_name != 'NULL' %}
        {{ event_cte.source_cte_name}}.{{ event_cte.project_column_name }}                                     AS dim_project_id,
        'project'                                                                                              AS parent_type,
        {{ event_cte.source_cte_name}}.{{ event_cte.project_column_name }}                                     AS parent_id,
        {{ event_cte.source_cte_name}}.ultimate_parent_namespace_id                                            AS ultimate_parent_namespace_id,
      {%- elif event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
        NULL                                                                                                   AS dim_project_id,
        'group'                                                                                                AS parent_type,
        {{ event_cte.source_cte_name}}.{{ event_cte.ultimate_parent_namespace_column_name }}                   AS parent_id,
        {{ event_cte.source_cte_name}}.ultimate_parent_namespace_id                                            AS ultimate_parent_namespace_id,
      {%- else %}
        NULL                                                                                                   AS dim_project_id,
        NULL                                                                                                   AS parent_type,
        NULL                                                                                                   AS parent_id,
        NULL                                                                                                   AS ultimate_parent_namespace_id,
      {%- endif %}
      {%- if event_cte.project_column_name != 'NULL' or event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
        COALESCE({{ event_cte.source_cte_name}}.{{event_cte.plan_column_name}}, 34)                            AS plan_id_at_event_timestamp,
        COALESCE(prep_plan.plan_name, 'free')                                                                  AS plan_name_at_event_timestamp,
        COALESCE(prep_plan.plan_is_paid, FALSE)                                                                AS plan_was_paid_at_event_timestamp,
      {%- else %}
        34                                                                                                     AS plan_id_at_event_timestamp,
        'free'                                                                                                 AS plan_name_at_event_timestamp,
        FALSE                                                                                                  AS plan_was_paid_at_event_timestamp,
      {%- endif %}
      {%- if event_cte.user_column_name != 'NULL' %}
        {{ event_cte.source_cte_name}}.{{ event_cte.user_column_name }}                                        AS dim_user_id,
        prep_user.created_at                                                                                   AS user_created_at,
        TO_DATE(prep_user.created_at)                                                                          AS user_created_date,
        FLOOR(
        DATEDIFF('day',
                prep_user.created_at::DATE,
                {{ event_cte.source_cte_name}}.created_at::DATE))                                              AS days_since_user_creation_at_event_date,
      {%- else %}
        NULL                                                                                                   AS dim_user_id,
        NULL                                                                                                   AS user_created_at,
        NULL                                                                                                   AS user_created_date,
        NULL                                                                                                   AS days_since_user_creation_at_event_date,
      {%- endif %}
      {%- if event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
        prep_namespace.created_at                                                                              AS namespace_created_at,
        TO_DATE(prep_namespace.created_at)                                                                     AS namespace_created_date,
        IFNULL(blocked_user.is_blocked_user, FALSE)                                                            AS is_blocked_namespace_creator,
        prep_namespace.namespace_is_internal                                                                   AS namespace_is_internal,
        FLOOR(
        DATEDIFF('day',
                prep_namespace.created_at::DATE,
                {{ event_cte.source_cte_name}}.created_at::DATE))                                              AS days_since_namespace_creation_at_event_date,
      {%- else %}
        NULL                                                                                                   AS namespace_created_at,
        NULL                                                                                                   AS namespace_created_date,
        NULL                                                                                                   AS is_blocked_namespace_creator,
        NULL                                                                                                   AS namespace_is_internal,
        NULL                                                                                                   AS days_since_namespace_creation_at_event_date,
      {%- endif %}
      {%- if event_cte.project_column_name != 'NULL' %}
        FLOOR(
        DATEDIFF('day',
                dim_project.created_at::DATE,
                {{ event_cte.source_cte_name}}.created_at::DATE))                                              AS days_since_project_creation_at_event_date,
        IFNULL(dim_project.is_imported, FALSE)                                                                 AS project_is_imported,
        dim_project.is_learn_gitlab                                                                            AS project_is_learn_gitlab
      {%- else %}
        NULL                                                                                                   AS days_since_project_creation_at_event_date,
        NULL                                                                                                   AS project_is_imported,
        NULL                                                                                                   AS project_is_learn_gitlab
      {%- endif %}
    FROM {{ event_cte.source_cte_name }}
    {%- if event_cte.project_column_name != 'NULL' %}
    LEFT JOIN dim_project
      ON {{event_cte.source_cte_name}}.{{event_cte.project_column_name}} = dim_project.dim_project_id
    {%- endif %}
    {%- if event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
    LEFT JOIN prep_namespace
      ON {{event_cte.source_cte_name}}.{{event_cte.ultimate_parent_namespace_column_name}} = prep_namespace.dim_namespace_id
      AND prep_namespace.is_currently_valid = TRUE
    LEFT JOIN prep_user AS blocked_user
      ON prep_namespace.creator_id = blocked_user.dim_user_id
    {%- endif %}
    {%- if event_cte.user_column_name != 'NULL' %}
    LEFT JOIN prep_user
      ON {{event_cte.source_cte_name}}.{{event_cte.user_column_name}} = prep_user.dim_user_id
    {%- endif %}
    {%- if event_cte.project_column_name != 'NULL' or event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
    LEFT JOIN prep_plan
      ON {{event_cte.source_cte_name}}.{{event_cte.plan_column_name}} = prep_plan.dim_plan_id
    {%- endif%}
    WHERE DATE_PART('year', {{ event_cte.source_cte_name}}.created_at) = {{year_value}}
      AND DATE_PART('month', {{ event_cte.source_cte_name}}.created_at) = {{month_value}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{%- endfor %}

)

SELECT *
FROM data
