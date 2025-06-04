{% set sensitive_fields = ['project_description', 'project_import_source', 'project_issues_template',
                           'project_name', 'project_path', 'project_import_url', 'project_merge_requests_template'] %}

WITH projects AS (

    SELECT *
    FROM {{ref('gitlab_ops_projects')}}

),

members AS (

    SELECT *
    FROM {{ref('gitlab_ops_members')}} members
    WHERE is_currently_valid = TRUE
      AND {{ filter_out_blocked_users('members', 'user_id') }}

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

      {% for field in sensitive_fields %}
      CASE
        WHEN projects.visibility_level != 'public' -- AND NOT namespace_lineage.namespace_is_internal -- missing table 
          THEN 'project is private/internal'
        ELSE {{field}}
      END                                                          AS {{field}},
      {% endfor %}
      COALESCE(COUNT(DISTINCT members.member_id), 0)               AS member_count
    FROM projects
      LEFT JOIN members
        ON projects.project_id = members.source_id
        AND members.member_source_type = 'Project'
    {{ dbt_utils.group_by(n=56) }}
)

SELECT *
FROM joined

