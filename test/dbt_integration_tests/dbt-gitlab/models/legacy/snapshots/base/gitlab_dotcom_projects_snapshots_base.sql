{{ config({
    "alias": "gitlab_dotcom_projects_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_projects_snapshots') }}

), renamed AS (

    SELECT

      id::NUMBER                                                                 AS project_id,
      description::VARCHAR                                                        AS project_description,
      import_source::VARCHAR                                                      AS project_import_source,
      issues_template::VARCHAR                                                    AS project_issues_template,
      name::VARCHAR                                                               AS project_name,
      path::VARCHAR                                                               AS project_path,
      import_url::VARCHAR                                                         AS project_import_url,
      merge_requests_template                                                     AS project_merge_requests_template,

      created_at::TIMESTAMP                                                       AS created_at,
      updated_at::TIMESTAMP                                                       AS updated_at,

      creator_id::NUMBER                                                          AS creator_id,
      namespace_id::NUMBER                                                        AS namespace_id,

      last_activity_at::TIMESTAMP                                                 AS last_activity_at,

      CASE
        WHEN visibility_level = '20' THEN 'public'
        WHEN visibility_level = '10' THEN 'internal'
        ELSE 'private'
      END::VARCHAR                                                                AS visibility_level,

      true::BOOLEAN                                                           AS archived,

      IFF(avatar IS NULL, FALSE, TRUE)::BOOLEAN                                   AS has_avatar,

      star_count::NUMBER                                                         AS project_star_count,
      true::BOOLEAN                                      AS merge_requests_rebase_enabled,
      IFF(LOWER(import_type) = 'nan', NULL, import_type)                          AS import_type,
      approvals_before_merge::NUMBER                                             AS approvals_before_merge,
      true::BOOLEAN                                            AS reset_approvals_on_push,
      true::BOOLEAN                                     AS merge_requests_ff_only_enabled,
      true::BOOLEAN                                                             AS mirror,
      mirror_user_id::NUMBER                                                     AS mirror_user_id,
      true::BOOLEAN                                             AS shared_runners_enabled,
      true::BOOLEAN                                              AS build_allow_git_fetch,
      build_timeout::NUMBER                                                      AS build_timeout,
      true::BOOLEAN                                              AS mirror_trigger_builds,
      true::BOOLEAN                                                     AS pending_delete,
      true::BOOLEAN                                                      AS public_builds,
      true::BOOLEAN                                       AS last_repository_check_failed,
      last_repository_check_at::TIMESTAMP                                         AS last_repository_check_at,
      true::BOOLEAN                                         AS container_registry_enabled,
      true::BOOLEAN                              AS only_allow_merge_if_pipeline_succeeds,
      true::BOOLEAN                                         AS has_external_issue_tracker,
      repository_storage,
      true::BOOLEAN                                               AS repository_read_only,
      true::BOOLEAN                                             AS request_access_enabled,
      true::BOOLEAN                                                  AS has_external_wiki,
      ci_config_path,
      true::BOOLEAN                                                        AS lfs_enabled,
      true::BOOLEAN                   AS only_allow_merge_if_all_discussions_are_resolved,
      repository_size_limit::NUMBER                                              AS repository_size_limit,
      true::BOOLEAN                                AS printing_merge_request_link_enabled,
      IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                  AS has_auto_canceling_pending_pipelines,
      true::BOOLEAN                                               AS service_desk_enabled,
      IFF(LOWER(delete_error) = 'nan', NULL, delete_error)                        AS delete_error,
      last_repository_updated_at::TIMESTAMP                                       AS last_repository_updated_at,
      storage_version::NUMBER                                                    AS storage_version,
      true::BOOLEAN                                  AS resolve_outdated_diff_discussions,
      true::BOOLEAN                     AS disable_overriding_approvers_per_merge_request,
      true::BOOLEAN                                 AS remote_mirror_available_overridden,
      true::BOOLEAN                                     AS only_mirror_protected_branches,
      true::BOOLEAN                                   AS pull_mirror_available_overridden,
      true::BOOLEAN                                AS mirror_overwrites_diverged_branches,
      external_authorization_classification_label,
      project_namespace_id::NUMBER AS project_namespace_id,
      dbt_valid_from::TIMESTAMP                                                   AS valid_from,
      dbt_valid_to::TIMESTAMP                                                     AS valid_to
    FROM source

)

SELECT *
FROM renamed
