

WITH namespace_lineage AS (
  SELECT
    *,
    IFF(lineage_valid_to::DATE = CURRENT_DATE(), {{ var('tomorrow') }}, lineage_valid_to)::DATE AS modified_lineage_valid_to_date,
    lineage_valid_from::DATE                                                                    AS lineage_valid_from_date
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_lineage_id, lineage_valid_from::DATE ORDER BY lineage_valid_from DESC) = 1

),

subscription_history AS (
  SELECT
    *,
    valid_from::DATE                                AS valid_from_date,
    COALESCE(valid_to, {{ var('tomorrow') }})::DATE AS valid_to_date
  FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY gitlab_subscription_id, valid_from::DATE ORDER BY valid_from DESC) = 1
),

namespace_history AS (
  SELECT
    *,
    valid_from::DATE                                AS valid_from_date,
    COALESCE(valid_to, {{ var('tomorrow') }})::DATE AS valid_to_date
  FROM {{ ref('prep_namespace_hist') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id, valid_from::DATE ORDER BY valid_from DESC) = 1

),

map_namespace_internal AS (

  SELECT *
  FROM {{ ref('map_namespace_internal') }}

),

plans AS (

  SELECT *
  FROM {{ ref('prep_gitlab_dotcom_plan') }}

),

missing_subscription_entry AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['CURRENT_DATE', 'gitlab_subscription_id'] ) }}
    AS new_gitlab_subscription_snapshot_id,
    gitlab_subscription_id,
    gitlab_subscription_start_date,
    gitlab_subscription_end_date,
    gitlab_subscription_trial_ends_on,
    namespace_id,
    34                                    AS plan_id,
    NULL                                  AS max_seats_used,
    NULL                                  AS seats,
    is_trial,
    gitlab_subscription_created_at,
    gitlab_subscription_updated_at,
    NULL                                  AS seats_in_use,
    seats_owed,
    trial_extension_type,
    valid_to                              AS new_valid_from,
    NULL                                  AS new_valid_to,
    valid_to_date                         AS new_valid_from_date,
    {{ var('tomorrow') }}::DATE           AS new_valid_to_date
  FROM subscription_history
  QUALIFY COUNT_IF(valid_to IS NULL) OVER (PARTITION BY namespace_id) = 0
  AND ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY valid_from DESC) = 1
),

corrected_subscription_history AS (

  SELECT *
  FROM subscription_history

  UNION ALL

  SELECT *
  FROM missing_subscription_entry

),

final AS (
  SELECT
    GREATEST(
      namespace_history.valid_from_date,
      namespace_lineage.lineage_valid_from_date,
      COALESCE(corrected_subscription_history.valid_from_date, '1900-01-01')
    )::DATE                                                                          AS combined_valid_from,
    LEAST(
      namespace_history.valid_to_date,
      namespace_lineage.modified_lineage_valid_to_date,
      COALESCE(corrected_subscription_history.valid_to_date, {{ var('tomorrow') }})
    )::DATE                                                                          AS combined_valid_to,
    IFF(combined_valid_to = {{ var('tomorrow') }}, TRUE, FALSE)                      AS is_current,
    namespace_history.dim_namespace_id,
    namespace_history.namespace_name,
    namespace_history.namespace_path,
    namespace_history.owner_id,
    namespace_history.namespace_type,
    namespace_history.has_avatar,
    namespace_history.namespace_created_at,
    namespace_history.namespace_updated_at,
    namespace_history.is_membership_locked,
    namespace_history.has_request_access_enabled,
    namespace_history.has_share_with_group_locked,
    namespace_history.visibility_level,
    namespace_history.ldap_sync_status,
    namespace_history.ldap_sync_error,
    namespace_history.ldap_sync_last_update_at,
    namespace_history.ldap_sync_last_successful_update_at,
    namespace_history.ldap_sync_last_sync_at,
    namespace_history.lfs_enabled,
    namespace_history.shared_runners_enabled,
    namespace_history.shared_runners_minutes_limit,
    namespace_history.extra_shared_runners_minutes_limit,
    namespace_history.repository_size_limit,
    namespace_history.does_require_two_factor_authentication,
    namespace_history.two_factor_grace_period,
    namespace_history.project_creation_level,
    namespace_history.push_rule_id,
    namespace_lineage.namespace_lineage_id,
    namespace_lineage.namespace_id,
    namespace_lineage.parent_id,
    namespace_lineage.upstream_lineage,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.lineage_depth,
    corrected_subscription_history.gitlab_subscription_id,
    corrected_subscription_history.gitlab_subscription_start_date,
    corrected_subscription_history.gitlab_subscription_end_date,
    corrected_subscription_history.gitlab_subscription_trial_ends_on,
    corrected_subscription_history.max_seats_used,
    corrected_subscription_history.seats,
    corrected_subscription_history.is_trial,
    corrected_subscription_history.gitlab_subscription_created_at,
    corrected_subscription_history.gitlab_subscription_updated_at,
    corrected_subscription_history.seats_in_use,
    corrected_subscription_history.seats_owed,
    corrected_subscription_history.trial_extension_type,
    CASE
      WHEN corrected_subscription_history.is_trial = TRUE AND LOWER(plans.plan_name_modified) = 'ultimate' THEN 102
      WHEN corrected_subscription_history.plan_id IS NULL THEN 34
      ELSE plans.plan_id_modified
    END                                                                              AS ultimate_parent_plan_id,
    ultimate_parent_plans.plan_title,
    ultimate_parent_plans.plan_is_paid,
    ultimate_parent_plans.plan_name,
    ultimate_parent_plans.plan_name_modified,
    COALESCE(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL, FALSE) AS namespace_is_internal
  FROM namespace_history
  LEFT JOIN namespace_lineage
    ON namespace_history.dim_namespace_id = namespace_lineage.namespace_id
      AND NOT (namespace_history.valid_to_date <= namespace_lineage.lineage_valid_from_date
        OR namespace_history.valid_from_date >= namespace_lineage.modified_lineage_valid_to_date)
  LEFT JOIN corrected_subscription_history
    ON namespace_lineage.ultimate_parent_id = corrected_subscription_history.namespace_id
      AND NOT (namespace_lineage.modified_lineage_valid_to_date <= corrected_subscription_history.valid_from_date
        OR namespace_lineage.lineage_valid_from_date >= corrected_subscription_history.valid_to_date
        OR namespace_history.valid_to_date <= corrected_subscription_history.valid_from_date
        OR namespace_history.valid_from_date >= corrected_subscription_history.valid_to_date)
  LEFT JOIN plans
    ON corrected_subscription_history.plan_id = plans.dim_plan_id
  LEFT JOIN plans AS ultimate_parent_plans
    ON ultimate_parent_plan_id = ultimate_parent_plans.dim_plan_id
  LEFT JOIN map_namespace_internal
    ON namespace_lineage.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
  WHERE combined_valid_from != combined_valid_to
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2023-08-14",
    updated_date="2023-08-14"
) }}
