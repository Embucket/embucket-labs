

{% set sensitive_fields = ['project_description', 'project_import_source', 'project_issues_template',
                           'project_name', 'project_path', 'project_import_url', 'project_merge_requests_template'] %}

WITH projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

),

namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

),

members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}} members
    WHERE is_currently_valid = TRUE
      AND {{ filter_out_blocked_users('members', 'user_id') }}

),

namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

),

gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}

),

active_services AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_integrations_source')}}
    WHERE is_active = True

),

joined AS (
    SELECT
      projects.project_id,
      projects.created_at, -- We will phase out `project_created_at`
      projects.created_at                                          AS project_created_at,
      projects.updated_at                                          AS project_updated_at,
      projects.creator_id,
      projects.namespace_id,
      projects.last_activity_at,
      projects.visibility_level,
      projects.archived,
      projects.has_avatar,
      projects.project_star_count,
      projects.merge_requests_rebase_enabled,
      projects.import_type,
      projects.approvals_before_merge,
      projects.reset_approvals_on_push,
      projects.merge_requests_ff_only_enabled,
      projects.mirror,
      projects.mirror_user_id,
      projects.shared_runners_enabled,
      projects.build_allow_git_fetch,
      projects.build_timeout,
      projects.mirror_trigger_builds,
      projects.pending_delete,
      projects.public_builds,
      projects.last_repository_check_failed,
      projects.last_repository_check_at,
      projects.container_registry_enabled,
      projects.only_allow_merge_if_pipeline_succeeds,
      projects.has_external_issue_tracker,
      projects.repository_storage,
      projects.repository_read_only,
      projects.request_access_enabled,
      projects.has_external_wiki,
      projects.ci_config_path,
      projects.lfs_enabled,
      projects.only_allow_merge_if_all_discussions_are_resolved,
      projects.repository_size_limit,
      projects.printing_merge_request_link_enabled,
      projects.has_auto_canceling_pending_pipelines,
      projects.service_desk_enabled,
      projects.delete_error,
      projects.last_repository_updated_at,
      projects.storage_version,
      projects.resolve_outdated_diff_discussions,
      projects.disable_overriding_approvers_per_merge_request,
      projects.remote_mirror_available_overridden,
      projects.only_mirror_protected_branches,
      projects.pull_mirror_available_overridden,
      projects.mirror_overwrites_diverged_branches,
      IFF(projects.import_type='gitlab_project' AND projects.project_path='learn-gitlab',
        TRUE,
        FALSE)                                                     AS is_learn_gitlab,

      {% for field in sensitive_fields %}
      CASE
        WHEN projects.visibility_level != 'public' AND NOT namespace_lineage.namespace_is_internal
          THEN 'project is private/internal'
        ELSE {{field}}
      END                                                          AS {{field}},
      {% endfor %}

      namespaces.namespace_name,
      namespaces.namespace_path,

      namespace_lineage.namespace_is_internal,
      namespace_lineage.namespace_plan_id,
      namespace_lineage.namespace_plan_title,
      namespace_lineage.namespace_plan_is_paid,
      namespace_lineage.ultimate_parent_id,
      namespace_lineage.ultimate_parent_plan_id,
      namespace_lineage.ultimate_parent_plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid,

      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END                                                          AS plan_id_at_project_creation,
      CASE
        WHEN import_type IS NULL
          THEN NULL
        WHEN import_type = 'gitlab_project' AND project_import_url IS NULL
          THEN 'project_template'
        WHEN import_type = 'gitlab_project' AND project_import_url IS NOT NULL
          THEN 'gitlab_project_import'
        WHEN import_type IS NOT NULL AND import_type != 'gitlab_project' AND project_import_url IS NOT NULL
          THEN 'other_source_project_import'
      END                                                          AS project_template,
      ARRAYAGG(active_services.integration_type)                   AS active_service_types,
      COALESCE(COUNT(DISTINCT members.member_id), 0)               AS member_count
    FROM projects
      LEFT JOIN members
        ON projects.project_id = members.source_id
        AND members.member_source_type = 'Project'
      LEFT JOIN namespaces
        ON projects.namespace_id = namespaces.namespace_id
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id
      LEFT JOIN gitlab_subscriptions
        ON namespace_lineage.ultimate_parent_id  = gitlab_subscriptions.namespace_id
        AND projects.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
      LEFT JOIN active_services
        ON projects.project_id = active_services.project_id
    {{ dbt_utils.group_by(n=69) }}
)

SELECT *
FROM joined
