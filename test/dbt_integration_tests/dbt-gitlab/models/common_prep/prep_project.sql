{% set sensitive_fields = ['project_description', 'project_import_source', 'project_issues_template',
                           'project_name', 'project_path', 'project_import_url', 'project_merge_requests_template'] %}

{{ simple_cte([

    ('dim_date', 'dim_date'),
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('members_source', 'gitlab_dotcom_members_source'),
    ('namespace_lineage_historical', 'gitlab_dotcom_namespace_lineage_historical_daily'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_namespace', 'prep_namespace'),
    ('projects_source', 'gitlab_dotcom_projects_source'),
    ('prep_product_tier', 'prep_product_tier'),
    ('gitlab_dotcom_project_ci_cd_settings_source', 'gitlab_dotcom_project_ci_cd_settings_source')

]) }}

, active_services AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_integrations_source')}}
    WHERE is_active = True

), namespace_lineage AS (

    SELECT
      namespace_lineage_historical.*,
      IFF(ROW_NUMBER() OVER (
        PARTITION BY namespace_lineage_historical.namespace_id
        ORDER BY namespace_lineage_historical.snapshot_day DESC) = 1, TRUE, FALSE)    AS is_current,
      namespace_lineage_historical.snapshot_day = CURRENT_DATE                        AS ultimate_parent_is_current,
      plans.plan_title                                                                AS ultimate_parent_plan_title,
      plans.plan_is_paid                                                              AS ultimate_parent_plan_is_paid,
      plans.plan_name                                                                 AS ultimate_parent_plan_name
    FROM namespace_lineage_historical
    INNER JOIN plans
      ON namespace_lineage_historical.ultimate_parent_plan_id = plans.plan_id
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        namespace_lineage_historical.namespace_id,
        namespace_lineage_historical.parent_id,
        namespace_lineage_historical.ultimate_parent_id
      ORDER BY namespace_lineage_historical.snapshot_day DESC
    ) = 1

), joined AS (

    SELECT

      -- SURROGATE KEY
      {{ ('projects_source.project_id') }}                           AS dim_project_sk,

      -- NATURAL KEY
      projects_source.project_id                                     AS project_id,

      -- LEGACY NATURAL KEY
      projects_source.project_id                                     AS dim_project_id,

      -- FOREIGN KEYS
      projects_source.namespace_id                                   AS dim_namespace_id,
      prep_namespace.dim_namespace_sk,
      namespace_lineage.ultimate_parent_id                           AS ultimate_parent_namespace_id,
      projects_source.creator_id                                     AS dim_user_id_creator,
      dim_date.date_id                                               AS dim_date_id,

      -- plan/product tier metadata at creation
      prep_namespace.dim_product_tier_id                             AS dim_product_tier_id_at_creation,
      prep_namespace.gitlab_plan_id                                  AS dim_plan_id,
      -- projects metadata
      projects_source.created_at                                     AS created_at,
      projects_source.updated_at                                     AS updated_at,
      projects_source.last_activity_at,
      projects_source.visibility_level,
      projects_source.archived                                       AS is_archived,
      projects_source.has_avatar,
      projects_source.project_star_count,
      projects_source.merge_requests_rebase_enabled,
      projects_source.import_type,
      IFF(projects_source.import_type IS NOT NULL, TRUE, FALSE)      AS is_imported,
      projects_source.approvals_before_merge,
      projects_source.reset_approvals_on_push,
      projects_source.merge_requests_ff_only_enabled,
      projects_source.mirror,
      projects_source.mirror_user_id,
      projects_source.shared_runners_enabled,
      projects_source.build_allow_git_fetch,
      projects_source.build_timeout,
      projects_source.mirror_trigger_builds,
      projects_source.pending_delete,
      projects_source.public_builds,
      projects_source.last_repository_check_failed,
      projects_source.last_repository_check_at,
      projects_source.container_registry_enabled,
      projects_source.only_allow_merge_if_pipeline_succeeds,
      projects_source.has_external_issue_tracker,
      projects_source.repository_storage,
      projects_source.repository_read_only,
      projects_source.request_access_enabled,
      projects_source.has_external_wiki,
      projects_source.ci_config_path,
      projects_source.lfs_enabled,
      projects_source.only_allow_merge_if_all_discussions_are_resolved,
      projects_source.repository_size_limit,
      projects_source.printing_merge_request_link_enabled,
      projects_source.has_auto_canceling_pending_pipelines,
      projects_source.service_desk_enabled,
      projects_source.delete_error,
      projects_source.last_repository_updated_at,
      projects_source.storage_version,
      projects_source.resolve_outdated_diff_discussions,
      projects_source.disable_overriding_approvers_per_merge_request,
      projects_source.remote_mirror_available_overridden,
      projects_source.only_mirror_protected_branches,
      projects_source.pull_mirror_available_overridden,
      projects_source.mirror_overwrites_diverged_branches,
      gitlab_dotcom_project_ci_cd_settings_source.merge_trains_enabled AS has_merge_trains_enabled,
      -- namespace metadata

      IFNULL(prep_namespace.namespace_is_internal, FALSE)            AS namespace_is_internal,

      {% for field in sensitive_fields %}
      CASE
        WHEN projects_source.visibility_level != 'public' AND NOT namespace_lineage.namespace_is_internal
          THEN 'project is private/internal'
        ELSE {{field}}
      END                                                            AS {{field}},
      {% endfor %}
      IFF(projects_source.import_type='gitlab_project' AND projects_source.project_path='learn-gitlab',  
        TRUE, 
        FALSE)                                                       AS is_learn_gitlab,
      ARRAYAGG(active_services.integration_type)                     AS active_service_types_array,

      IFNULL(COUNT(DISTINCT members_source.member_id), 0)            AS member_count
    FROM projects_source
    INNER JOIN dim_date
      ON TO_DATE(projects_source.created_at) = dim_date.date_day
    LEFT JOIN prep_namespace
      ON projects_source.namespace_id = prep_namespace.dim_namespace_id
      AND prep_namespace.is_currently_valid
    LEFT JOIN members_source
      ON projects_source.project_id = members_source.source_id
      AND members_source.member_source_type = 'Project'
    LEFT JOIN namespace_lineage
      ON prep_namespace.dim_namespace_id = namespace_lineage.namespace_id
      AND namespace_lineage.is_current = TRUE
    LEFT JOIN gitlab_subscriptions
      ON namespace_lineage.ultimate_parent_id  = gitlab_subscriptions.namespace_id
        AND projects_source.created_at >= gitlab_subscriptions.valid_from AND projects_source.created_at < {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
    LEFT JOIN active_services
      ON projects_source.project_id = active_services.project_id
    LEFT JOIN gitlab_dotcom_project_ci_cd_settings_source
      ON projects_source.project_id = gitlab_dotcom_project_ci_cd_settings_source.project_id
    {{ dbt_utils.group_by(n=66) }}

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-03-17",
    updated_date="2024-04-10"
) }}

