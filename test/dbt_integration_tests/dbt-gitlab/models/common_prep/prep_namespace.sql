{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('namespaces', 'gitlab_dotcom_namespaces_source'),
    ('namespace_snapshots', 'prep_namespace_hist'),
    ('namespace_settings', 'gitlab_dotcom_namespace_settings_source'),
    ('namespace_lineage', 'gitlab_dotcom_namespace_lineage'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('product_tiers', 'prep_product_tier'),
    ('members_source', 'gitlab_dotcom_members_source'),
    ('projects_source', 'gitlab_dotcom_projects_source'),
    ('audit_events', 'gitlab_dotcom_audit_events_source'),
    ('audit_event_details', 'gitlab_dotcom_audit_event_details'),
    ('users', 'prep_user')
]) }},

members AS (

  SELECT
    source_id,
    COUNT(DISTINCT member_id) AS member_count
  FROM members_source
  WHERE is_currently_valid = TRUE
    AND member_source_type = 'Namespace'
    AND {{ filter_out_blocked_users('members_source', 'user_id') }}
  GROUP BY 1

),

projects AS (

  SELECT
    namespace_id,
    COUNT(DISTINCT project_id) AS project_count
  FROM projects_source
  GROUP BY 1

),

creators AS (

  SELECT
    audit_events.author_id AS creator_id,
    audit_events.entity_id AS group_id
  FROM audit_events
  INNER JOIN audit_event_details
    ON audit_events.audit_event_id = audit_event_details.audit_event_id
  WHERE audit_events.entity_type = 'Group'
    AND (
      (audit_event_details.key_name = 'add' AND audit_event_details.key_value = 'group')
      OR
      (audit_event_details.key_name = 'custom_message' AND audit_event_details.key_value = 'Added group')
    )
  GROUP BY 1, 2

),

joined AS (

  SELECT

    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['namespaces.namespace_id']) }}              AS dim_namespace_sk,

    -- Natural Key
    namespaces.namespace_id                                                 AS namespace_id,

    -- Legacy Natural Key
    namespaces.namespace_id                                                 AS dim_namespace_id,

    -- Foreign Keys
    namespaces.owner_id,
    namespaces.parent_id,
    COALESCE(creators.creator_id, namespaces.owner_id)                      AS creator_id,
    namespace_lineage.ultimate_parent_plan_id                               AS gitlab_plan_id,
    COALESCE(namespace_lineage.ultimate_parent_id,
      namespaces.parent_id,
      namespaces.namespace_id)                                              AS ultimate_parent_namespace_id,
    {{ get_keyed_nulls('saas_product_tiers.dim_product_tier_id') }}         AS dim_product_tier_id,

    -- Attributes
    IFF(namespaces.namespace_id = COALESCE(namespace_lineage.ultimate_parent_id,
      namespaces.parent_id,
      namespaces.namespace_id),
      TRUE, FALSE)                                                          AS namespace_is_ultimate_parent,
    IFF(internal_namespaces.namespace_is_internal = TRUE, TRUE, FALSE)      AS namespace_is_internal,
    CASE
      WHEN namespaces.visibility_level = 'public'
        OR namespaces.namespace_is_internal THEN namespaces.namespace_name
      WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
      WHEN namespaces.visibility_level = 'private' THEN 'private - masked'
    END                                                                     AS namespace_name,
    namespaces.namespace_name                                               AS namespace_name_unmasked,
    CASE
      WHEN namespaces.visibility_level = 'public'
        OR namespaces.namespace_is_internal THEN namespaces.namespace_path
      WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
      WHEN namespaces.visibility_level = 'private' THEN 'private - masked'
    END                                                                     AS namespace_path,
    namespaces.namespace_type                                               AS namespace_type,
    namespaces.has_avatar,
    namespaces.created_at                                                   AS created_at,
    namespaces.updated_at                                                   AS updated_at,
    namespaces.is_membership_locked,
    namespaces.has_request_access_enabled,
    namespaces.has_share_with_group_locked,
    namespace_settings.is_setup_for_company,
    namespaces.visibility_level,
    namespaces.ldap_sync_status,
    namespaces.ldap_sync_error,
    namespaces.ldap_sync_last_update_at,
    namespaces.ldap_sync_last_successful_update_at,
    namespaces.ldap_sync_last_sync_at,
    namespaces.lfs_enabled,
    namespaces.shared_runners_enabled,
    namespaces.shared_runners_minutes_limit,
    namespaces.extra_shared_runners_minutes_limit,
    namespaces.repository_size_limit,
    namespaces.does_require_two_factor_authentication,
    namespaces.two_factor_grace_period,
    namespaces.project_creation_level,
    namespaces.push_rule_id,
    COALESCE(users.is_blocked_user, FALSE)                                  AS namespace_creator_is_blocked,
    namespace_lineage.ultimate_parent_plan_title                            AS gitlab_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid                          AS gitlab_plan_is_paid,
    namespace_lineage.seats                                                 AS gitlab_plan_seats,
    namespace_lineage.seats_in_use                                          AS gitlab_plan_seats_in_use,
    namespace_lineage.max_seats_used                                        AS gitlab_plan_max_seats_used,
    COALESCE(members.member_count, 0)                                       AS namespace_member_count,
    COALESCE(projects.project_count, 0)                                     AS namespace_project_count,
    namespace_settings.code_suggestions                                     AS has_code_suggestions_enabled,
    -- keep as legacy field until code can be refactored
    TRUE                                                                    AS is_currently_valid,
    namespaces.is_deleted                                                   AS is_deleted,
    namespaces.is_deleted_updated_at                                        AS is_deleted_updated_at
  FROM namespaces
  LEFT JOIN namespace_lineage
    ON namespaces.namespace_id = namespace_lineage.namespace_id
      AND COALESCE(namespaces.parent_id, namespaces.namespace_id) = COALESCE(namespace_lineage.parent_id, namespace_lineage.namespace_id)
  LEFT JOIN namespace_settings
    ON namespaces.namespace_id = namespace_settings.namespace_id
  LEFT JOIN members
    ON namespaces.namespace_id = members.source_id
  LEFT JOIN projects
    ON namespaces.namespace_id = projects.namespace_id
  LEFT JOIN creators
    ON namespaces.namespace_id = creators.group_id
  LEFT JOIN users
    ON creators.creator_id = users.dim_user_id
  LEFT JOIN namespaces internal_namespaces
    ON namespace_lineage.ultimate_parent_id = internal_namespaces.namespace_id
  LEFT JOIN product_tiers saas_product_tiers
    ON saas_product_tiers.product_deployment_type = 'GitLab.com'
      AND namespace_lineage.ultimate_parent_plan_name = LOWER(IFF(saas_product_tiers.product_tier_name_short != 'Trial: Ultimate',
        saas_product_tiers.product_tier_historical_short,
        'ultimate_trial'))

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@michellecooper",
    created_date="2021-01-14",
    updated_date="2024-11-15"
) }}
