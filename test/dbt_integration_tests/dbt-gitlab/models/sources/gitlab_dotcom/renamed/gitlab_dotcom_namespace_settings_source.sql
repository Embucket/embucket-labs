
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_settings_dedupe_source') }}

), renamed AS (

    SELECT
      created_at::TIMESTAMP                               AS created_at,
      updated_at::TIMESTAMP                               AS updated_at,
      namespace_id::NUMBER                                AS namespace_id,
      prevent_forking_outside_group::BOOLEAN              AS prevent_forking_outside_group,
      allow_mfa_for_subgroups::BOOLEAN                    AS allow_mfa_for_subgroups,
      default_branch_name::VARCHAR                        AS default_branch_name,
      repository_read_only::BOOLEAN                       AS repository_read_only,
      resource_access_token_creation_allowed::BOOLEAN     AS resource_access_token_creation_allowed,
      prevent_sharing_groups_outside_hierarchy::BOOLEAN   AS prevent_sharing_groups_outside_hierarchy,
      new_user_signups_cap::NUMBER                        AS new_signups_cap,
      setup_for_company::BOOLEAN                          AS is_setup_for_company,
      jobs_to_be_done::NUMBER                             AS jobs_to_be_done,
      early_access_program_participant::BOOLEAN           AS early_access_program_participant,
      early_access_program_joined_by_id::NUMBER           AS early_access_program_joined_by_id,
      experiment_features_enabled::BOOLEAN                AS experiment_features_enabled,
      code_suggestions::BOOLEAN                           AS code_suggestions,
      seat_control::NUMBER                                AS seat_control
    FROM source

)

SELECT *
FROM renamed
