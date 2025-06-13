{{ config({
    "alias": "gitlab_dotcom_namespaces_xf",
    "snowflake_warehouse": generate_warehouse_name('XL')
}) }}

{% set fields_to_mask = ['namespace_name', 'namespace_path'] %}

WITH namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

),

members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}} members
    WHERE is_currently_valid = TRUE
      AND {{ filter_out_blocked_users('members', 'user_id') }}

),

projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

), creators AS (

    SELECT DISTINCT
      author_id AS creator_id,
      entity_id AS group_id
    FROM {{ ref('gitlab_dotcom_audit_event_details') }} AS audit_event_details
    LEFT JOIN {{ ref('gitlab_dotcom_audit_events') }} AS audit_events
        ON audit_event_details.audit_event_id = audit_events.audit_event_id
    WHERE entity_type = 'Group'
      AND key_name = 'add'
      AND key_value = 'group'

), joined AS (
    SELECT
      namespaces.namespace_id,

      {% for field in fields_to_mask %}
      CASE
        WHEN namespaces.visibility_level = 'public' OR namespace_lineage.namespace_is_internal THEN {{field}}
        WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
        WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END                                                              AS {{field}},
      {% endfor %}

      namespaces.owner_id,
      namespaces.namespace_type                                        AS namespace_type,
      namespaces.has_avatar,
      namespaces.created_at                                            AS namespace_created_at,
      namespaces.updated_at                                            AS namespace_updated_at,
      namespaces.is_membership_locked,
      namespaces.has_request_access_enabled,
      namespaces.has_share_with_group_locked,
      namespaces.visibility_level,
      namespaces.ldap_sync_status,
      namespaces.ldap_sync_error,
      namespaces.ldap_sync_last_update_at,
      namespaces.ldap_sync_last_successful_update_at,
      namespaces.ldap_sync_last_sync_at,
      namespaces.lfs_enabled,
      namespaces.parent_id,
      namespaces.shared_runners_enabled,
      namespaces.shared_runners_minutes_limit,
      namespaces.extra_shared_runners_minutes_limit,
      namespaces.repository_size_limit,
      namespaces.does_require_two_factor_authentication,
      namespaces.two_factor_grace_period,
      namespaces.project_creation_level,
      namespaces.push_rule_id,
      COALESCE(creators.creator_id, namespaces.owner_id)               AS creator_id,

      namespace_lineage.namespace_is_internal,
      namespace_lineage.ultimate_parent_id                             AS namespace_ultimate_parent_id,
      namespace_lineage.ultimate_parent_plan_id                        AS plan_id,
      namespace_lineage.ultimate_parent_plan_title                     AS plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid                   AS plan_is_paid,

      COALESCE(COUNT(DISTINCT members.member_id), 0)                   AS member_count,
      COALESCE(COUNT(DISTINCT projects.project_id), 0)                 AS project_count

    FROM namespaces
      LEFT JOIN members
        ON namespaces.namespace_id = members.source_id
        AND members.member_source_type = 'Namespace'
      LEFT JOIN projects
        ON namespaces.namespace_id = projects.namespace_id
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id
      LEFT JOIN creators
        ON namespaces.namespace_id = creators.group_id
    {{ dbt_utils.group_by(n=33) }}
)

SELECT *
FROM joined
